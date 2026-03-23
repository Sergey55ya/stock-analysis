# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from collections import defaultdict
import os
import requests
import sys
import logging
from datetime import datetime
import json

# ============================================
# 1. ÍÀÑÒÐÎÉÊÈ ËÎÃÈÐÎÂÀÍÈß
# ============================================

# Íàñòðîéêà ëîãèðîâàíèÿ
log_filename = f'analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ============================================
# 2. ÍÀÑÒÐÎÉÊÈ - ÑÑÛËÊÈ ÍÀ ÔÀÉËÛ
# ============================================

STOCK_FILE_URLS = [
    "https://admin.silam.ru/system/unload_prices/18/zzap.xlsx?rand=72e5c3fc-ec9e-4bc5-be8e-8dc237839f5f",
    "https://docs.google.com/spreadsheets/d/1PtOOfFrJIdEqLsiJiwOWfKw6BWjburyw/export?format=xlsx"
]

STOCK_FILENAMES = ["zzap_1.xlsx", "vse_lozhementy.xlsx"]

# ============================================
# 3. ÔÓÍÊÖÈÈ (òå æå ñàìûå, ÷òî â âàøåì ñêðèïòå)
# ============================================

def download_file(url, filename):
    """Ñêà÷èâàåò ôàéë ïî óêàçàííîé ññûëêå"""
    logger.info(f"?? Ïîïûòêà ñêà÷àòü ôàéë {filename}...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*',
        }
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type and 'google' not in url:
                logger.warning(f"   ?? Ñåðâåð âåðíóë HTML-ñòðàíèöó äëÿ {filename}")
                return False
            with open(filename, 'wb') as f:
                f.write(response.content)
            logger.info(f"   ? Ôàéë óñïåøíî ñêà÷àí: {filename} ({len(response.content)} áàéò)")
            return True
        else:
            logger.error(f"   ? Îøèáêà ïðè ñêà÷èâàíèè {filename}: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"   ? Îøèáêà ïðè ñêà÷èâàíèè {filename}: {e}")
        return False

def load_stock_file(filename):
    """Çàãðóæàåò îäèí ôàéë ñêëàäà è âîçâðàùàåò DataFrame"""
    try:
        df = pd.read_excel(filename, sheet_name=0, header=0)
        expected_columns = ['Êîä', 'Áðåíä', 'Íàèìåíîâàíèå', 'Öåíà', 'ID_ïîñòàâùèêà', 'Íàëè÷èå', 'Ñðîê']
        
        if len(df.columns) != 7:
            logger.warning(f"   ?? Íåñòàíäàðòíîå êîëè÷åñòâî êîëîíîê: {len(df.columns)}")
            df = pd.read_excel(filename, sheet_name=0, header=None, skiprows=1)
            if len(df.columns) >= 7:
                df = df.iloc[:, :7]
                df.columns = expected_columns
            else:
                logger.error(f"   ? Íå óäàëîñü îïðåäåëèòü ñòðóêòóðó ôàéëà")
                return pd.DataFrame()
        else:
            df.columns = expected_columns
            
        df = df.dropna(subset=['Êîä']).copy()
        df['Êîä'] = df['Êîä'].astype(str).str.strip()
        
        df['Öåíà'] = df['Öåíà'].astype(str).str.replace(',', '.').str.replace(' ', '')
        df['Öåíà'] = pd.to_numeric(df['Öåíà'], errors='coerce')
        df['Íàëè÷èå'] = pd.to_numeric(df['Íàëè÷èå'], errors='coerce').fillna(0)
        df['Ñðîê'] = pd.to_numeric(df['Ñðîê'], errors='coerce').fillna(999)
        
        logger.info(f"   ? Çàãðóæåíî {len(df)} ñòðîê èç {filename}")
        return df
    except Exception as e:
        logger.error(f"   ? Îøèáêà ïðè ÷òåíèè ôàéëà {filename}: {e}")
        return pd.DataFrame()

def clean_kit_name(full_name):
    """Î÷èùàåò íàçâàíèå êîìïëåêòà"""
    if not isinstance(full_name, str):
        return full_name
    name = full_name.strip()
    if ' /' in name:
        return name.rsplit(' /', 1)[0].strip()
    if name.endswith('/'):
        return name[:-1].strip()
    return name

def find_stock_items(article, df_stock):
    """Ïîèñê àðòèêóëîâ ñ íîðìàëèçàöèåé"""
    if df_stock.empty:
        return pd.DataFrame()
    
    article_upper = article.upper().strip()
    
    result = df_stock[df_stock['Êîä'].str.upper() == article_upper]
    if not result.empty:
        return result

    normalized = article_upper.replace('-', '')
    result = df_stock[df_stock['Êîä'].str.upper() == normalized]
    if not result.empty:
        logger.debug(f"      ?? Íàéäåí {article} êàê {normalized}")
        return result

    return pd.DataFrame()

def parse_all_kits_from_file(filename):
    """Ïàðñèò ôàéë ñî âñåìè êîìïëåêòàìè"""
    logger.info(f"?? Çàãðóçêà êîìïëåêòîâ èç ôàéëà {filename}...")
    
    try:
        df = pd.read_excel(filename, sheet_name=0, header=None)
        logger.info(f"   Âñåãî ñòðîê â ôàéëå: {len(df)}")

        kits = {}
        current_kit = None
        kit_components = []
        kit_name = ""
        kit_article = ""

        i = 0
        while i < len(df):
            row = df.iloc[i].astype(str).tolist()

            if len(row) > 1 and 'Êîìïëåêò' in str(row[1]):
                if i + 1 < len(df):
                    next_row = df.iloc[i+1].astype(str).tolist()
                    if len(next_row) > 2:
                        potential_name = str(next_row[1]).strip()
                        potential_article = str(next_row[2]).strip()
                        
                        if (potential_name and potential_name != 'nan' and
                            potential_article and potential_article != 'nan' and
                            len(potential_article) > 3):
                            
                            if current_kit and len(kit_components) > 0:
                                unique_components = []
                                seen = set()
                                for comp in kit_components:
                                    if comp not in seen and comp not in ['nan', 'Àðòèêóë']:
                                        seen.add(comp)
                                        unique_components.append(comp)
                                
                                if len(unique_components) > 0:
                                    clean_name = clean_kit_name(kit_name)
                                    kits[kit_article] = {
                                        'name': clean_name,
                                        'components': unique_components
                                    }
                                    logger.info(f"      ? Çàãðóæåí êîìïëåêò {kit_article}: {len(unique_components)} êîìïîíåíòîâ")
                            
                            kit_name = potential_name
                            kit_article = potential_article
                            kit_components = []
                            current_kit = kit_article
                            i += 2
                            continue

            if current_kit and len(row) > 2:
                article = str(row[2]).strip()
                if (article and article != 'nan' and article != 'Àðòèêóë' and
                    not article.startswith('ÓÒ') and len(article) > 1 and len(article) < 30):
                    exclude_words = ['ãîôðîÿùèê', 'ýòèêåòêà', 'ëîæåìåíò', 'íàèìåíîâàíèå',
                                     'êîìïëåêò', 'áðåíä', 'êîä', 'óïàêîâêà', 'êîðîáêà']
                    article_lower = article.lower()
                    if not any(word in article_lower for word in exclude_words):
                        kit_components.append(article)

            i += 1

        if current_kit and len(kit_components) > 0:
            unique_components = []
            seen = set()
            for comp in kit_components:
                if comp not in seen and comp not in ['nan', 'Àðòèêóë']:
                    seen.add(comp)
                    unique_components.append(comp)
            
            if len(unique_components) > 0:
                clean_name = clean_kit_name(kit_name)
                kits[kit_article] = {
                    'name': clean_name,
                    'components': unique_components
                }
                logger.info(f"      ? Çàãðóæåí êîìïëåêò {kit_article}: {len(unique_components)} êîìïîíåíòîâ")

        logger.info(f"\n   ? Âñåãî çàãðóæåíî êîìïëåêòîâ: {len(kits)}")
        return kits
    except Exception as e:
        logger.error(f"   ? Îøèáêà ïðè çàãðóçêå ôàéëà: {e}")
        return {}

def calculate_max_quantity_with_groups(components, df_stock, kit_article):
    """Ðàññ÷èòàòü ìàêñèìàëüíîå êîëè÷åñòâî êîìïëåêòîâ"""
    if df_stock.empty:
        return 0, [], None, None

    available_items = {}
    missing_articles = []

    for article in components:
        items = find_stock_items(article, df_stock)
        if items.empty:
            missing_articles.append(article)
            continue
        
        available = items[items['Íàëè÷èå'] > 0].copy()
        if available.empty:
            missing_articles.append(article)
            continue
        
        available = available[pd.notna(available['Öåíà'])]
        if available.empty:
            missing_articles.append(article)
            continue
        
        available = available.sort_values(['Ñðîê', 'Öåíà'])
        available_items[article] = available.to_dict('records')

    if missing_articles:
        logger.warning(f"      ?? Îòñóòñòâóþò êîìïîíåíòû: {missing_articles[:5]}...")
        return 0, [], missing_articles[0] if missing_articles else None, 0

    limiting_article = None
    limiting_qty = float('inf')
    
    for article, items in available_items.items():
        total_qty = sum(item['Íàëè÷èå'] for item in items)
        if total_qty < limiting_qty:
            limiting_qty = total_qty
            limiting_article = article

    max_kits = limiting_qty
    
    if max_kits == 0 or max_kits == float('inf'):
        return 0, [], limiting_article, limiting_qty

    stock_copies = {}
    for article, items in available_items.items():
        stock_copies[article] = []
        for item in items:
            stock_copies[article].append({
                'source': f"{item.get('ID_ïîñòàâùèêà', '?')}",
                'price': item['Öåíà'],
                'delivery': item['Ñðîê'],
                'qty': item['Íàëè÷èå']
            })

    kits_assembled = []
    
    for kit_num in range(int(max_kits)):
        kit_price = 0
        kit_delivery = 0
        kit_complete = True
        
        for article in components:
            found = False
            if article in stock_copies:
                for i, source in enumerate(stock_copies[article]):
                    if source['qty'] > 0:
                        kit_price += source['price']
                        if source['delivery'] > kit_delivery:
                            kit_delivery = source['delivery']
                        stock_copies[article][i]['qty'] -= 1
                        found = True
                        break
            if not found:
                kit_complete = False
                break
        
        if kit_complete:
            kits_assembled.append({
                'price': round(kit_price, 2),
                'delivery': kit_delivery
            })
    
    grouped = defaultdict(int)
    for kit in kits_assembled:
        key = (kit['price'], kit['delivery'])
        grouped[key] += 1
    
    result_groups = []
    for (price, delivery), count in sorted(grouped.items()):
        result_groups.append({
            'count': count,
            'price': price,
            'delivery': delivery
        })
    
    return max_kits, result_groups, limiting_article, limiting_qty

# ============================================
# 4. ÎÑÍÎÂÍÀß ÔÓÍÊÖÈß
# ============================================

def main():
    """Îñíîâíàÿ ôóíêöèÿ àíàëèçà"""
    logger.info("="*70)
    logger.info("?? ÇÀÏÓÑÊ ÀÍÀËÈÇÀ ÑÊËÀÄÑÊÈÕ ÎÑÒÀÒÊÎÂ")
    logger.info(f"?? Äàòà çàïóñêà: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    # Çàãðóçêà ôàéëîâ ñêëàäà
    logger.info("?? Çàãðóçêà ôàéëîâ ñêëàäñêèõ îñòàòêîâ...")
    all_stock_dfs = []
    
    for i, (url, filename) in enumerate(zip(STOCK_FILE_URLS, STOCK_FILENAMES)):
        logger.info(f"\n?? Ôàéë {i+1}: {filename}")
        
        if os.path.exists(filename):
            logger.info(f"   ? Ôàéë {filename} óæå ñóùåñòâóåò")
            file_exists = True
        else:
            file_exists = download_file(url, filename)
        
        if file_exists:
            df = load_stock_file(filename)
            if not df.empty:
                all_stock_dfs.append(df)
    
    if all_stock_dfs:
        df_stock = pd.concat(all_stock_dfs, ignore_index=True)
        df_stock = df_stock.drop_duplicates(subset=['Êîä', 'ID_ïîñòàâùèêà', 'Öåíà'], keep='first')
        logger.info(f"\n? Âñåãî çàãðóæåíî: {len(df_stock)} ñòðîê èç {len(all_stock_dfs)} ôàéëîâ")
    else:
        logger.error("?? Íå óäàëîñü çàãðóçèòü íè îäíîãî ôàéëà ñêëàäà")
        df_stock = pd.DataFrame(columns=['Êîä', 'Öåíà', 'Íàëè÷èå', 'Ñðîê', 'ID_ïîñòàâùèêà'])
    
    # Çàãðóçêà êîìïëåêòîâ
    kits_file = 'vse_lozhementy.xlsx'
    if not os.path.exists(kits_file):
        for filename in STOCK_FILENAMES:
            if 'lozhement' in filename.lower() or 'ëîæåìåíò' in filename.lower():
                kits_file = filename
                break
    
    if not os.path.exists(kits_file):
        logger.error(f"? Ôàéë ñ êîìïëåêòàìè íå íàéäåí!")
        return
    
    kits = parse_all_kits_from_file(kits_file)
    
    if not kits:
        logger.error("? Íåò çàãðóæåííûõ êîìïëåêòîâ äëÿ àíàëèçà!")
        return
    
    # Àíàëèç
    logger.info("\n?? ÀÍÀËÈÇ ÊÎÌÏËÅÊÒÎÂ")
    logger.info("="*70)
    
    all_results = []
    
    for kit_article, kit_info in kits.items():
        logger.info(f"\n?? Àíàëèç {kit_article}...")
        
        max_qty, groups, limiting_art, limiting_qty = calculate_max_quantity_with_groups(
            kit_info['components'], df_stock, kit_article
        )
        
        # Çàãîëîâîê
        all_results.append({
            'Êîìïëåêò': kit_info['name'],
            'Àðòèêóë': kit_article,
            'Áðåíä': 'PowerMechanics',
            'Êîëè÷åñòâî': '',
            'Öåíà': '',
            'Ñðîê': ''
        })
        
        # Ðåçóëüòàòû
        if max_qty > 0 and groups:
            for group in groups:
                all_results.append({
                    'Êîìïëåêò': kit_info['name'],
                    'Àðòèêóë': kit_article,
                    'Áðåíä': 'PowerMechanics',
                    'Êîëè÷åñòâî': group['count'],
                    'Öåíà': f"{group['price']:.2f} ?",
                    'Ñðîê': str(group['delivery'])
                })
            
            all_results.append({
                'Êîìïëåêò': 'Âñåãî êîìïëåêòîâ ïî íàëè÷èþ:',
                'Àðòèêóë': '',
                'Áðåíä': '',
                'Êîëè÷åñòâî': max_qty,
                'Öåíà': '',
                'Ñðîê': ''
            })
        else:
            all_results.append({
                'Êîìïëåêò': kit_info['name'],
                'Àðòèêóë': kit_article,
                'Áðåíä': 'PowerMechanics',
                'Êîëè÷åñòâî': 0,
                'Öåíà': '',
                'Ñðîê': ''
            })
        
        all_results.append({'Êîìïëåêò': '', 'Àðòèêóë': '', 'Áðåíä': '', 'Êîëè÷åñòâî': '', 'Öåíà': '', 'Ñðîê': ''})
    
    # Ñîõðàíåíèå ðåçóëüòàòîâ
    output_filename = f'results_{datetime.now().strftime("%Y%m%d")}.csv'
    df_results = pd.DataFrame(all_results)
    df_results.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    logger.info(f"\n?? Ðåçóëüòàòû ñîõðàíåíû â ôàéë: {output_filename}")
    logger.info(f"?? Ïðîàíàëèçèðîâàíî êîìïëåêòîâ: {len(kits)}")
    logger.info("? Àíàëèç çàâåðøåí óñïåøíî!")
    
    # Ñîçäàåì ôàéë ñ ìåòàäàííûìè
    metadata = {
        'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'kits_analyzed': len(kits),
        'stock_rows': len(df_stock),
        'output_file': output_filename
    }
    
    with open('metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"? Êðèòè÷åñêàÿ îøèáêà: {e}", exc_info=True)
        sys.exit(1)
