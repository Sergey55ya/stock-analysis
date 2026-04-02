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
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# ============================================
# 1. НАСТРОЙКИ ЛОГИРОВАНИЯ
# ============================================

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
# 2. НАСТРОЙКИ - ССЫЛКИ НА ФАЙЛЫ
# ============================================

STOCK_FILE_URLS = [
    "https://admin.silam.ru/system/unload_prices/18/zzap.xlsx?rand=72e5c3fc-ec9e-4bc5-be8e-8dc237839f5f",
    "https://docs.google.com/spreadsheets/d/1PtOOfFrJIdEqLsiJiwOWfKw6BWjburyw/export?format=xlsx"
]

STOCK_FILENAMES = ["zzap_1.xlsx", "vse_lozhementy.xlsx"]

# ============================================
# 3. ФУНКЦИЯ НОРМАЛИЗАЦИИ СТРОК (УЛУЧШЕННАЯ)
# ============================================

def normalize_string(s):
    """
    Полная нормализация строки для поиска:
    - Удаление пробелов в начале и конце
    - Приведение к верхнему регистру
    - Замена русских букв на английские
    - Удаление всех пробелов
    - Удаление дефисов, точек, запятых
    """
    if not isinstance(s, str):
        return ""
    
    # Приводим к верхнему регистру
    s = s.upper().strip()
    
    # Замена русских букв на английские
    replacements = {
        'А': 'A', 'В': 'B', 'Е': 'E', 'Ё': 'E', 'К': 'K', 'М': 'M',
        'Н': 'H', 'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y',
        'Х': 'X', 'а': 'A', 'в': 'B', 'е': 'E', 'ё': 'E', 'к': 'K',
        'м': 'M', 'н': 'H', 'о': 'O', 'р': 'P', 'с': 'C', 'т': 'T',
        'у': 'Y', 'х': 'X'
    }
    for rus, eng in replacements.items():
        s = s.replace(rus, eng)
    
    # Удаляем все пробелы
    s_no_spaces = s.replace(' ', '')
    
    # Удаляем дефисы, точки, запятые, слеши
    s_clean = re.sub(r'[-\.,/]', '', s_no_spaces)
    
    return s_clean

# ============================================
# 4. УЛУЧШЕННАЯ ФУНКЦИЯ ПОИСКА АРТИКУЛОВ
# ============================================

def find_stock_items(article, df_stock):
    """
    Поиск артикулов с улучшенной нормализацией
    """
    if df_stock.empty:
        return pd.DataFrame()
    
    original_article = article.strip()
    normalized_article = normalize_string(original_article)
    
    logger.debug(f"      🔍 Поиск: '{original_article}' -> нормализовано: '{normalized_article}'")
    
    # 1. Прямой поиск (как есть)
    result = df_stock[df_stock['Код'] == original_article]
    if not result.empty:
        logger.info(f"      ✅ Найден точное совпадение: {original_article}")
        return result
    
    # 2. Поиск без учета регистра
    result = df_stock[df_stock['Код'].str.upper() == original_article.upper()]
    if not result.empty:
        logger.info(f"      ✅ Найден без учета регистра: {original_article}")
        return result
    
    # 3. Поиск по нормализованному значению (без пробелов, без дефисов, с заменой букв)
    df_stock['Код_норм'] = df_stock['Код'].astype(str).apply(normalize_string)
    result = df_stock[df_stock['Код_норм'] == normalized_article]
    if not result.empty:
        found_code = result.iloc[0]['Код']
        logger.info(f"      ✅ Найден по нормализации: '{original_article}' -> '{found_code}'")
        return result
    
    # 4. Поиск по частичному совпадению (если нормализованный артикул длинный)
    if len(normalized_article) > 6:
        # Ищем, содержится ли наш артикул в артикуле из склада
        matches = df_stock[df_stock['Код_норм'].str.contains(normalized_article, na=False)]
        if len(matches) == 1:
            found_code = matches.iloc[0]['Код']
            logger.info(f"      ✅ Найден по частичному совпадению: '{original_article}' -> '{found_code}'")
            return matches
        
        # Ищем, содержится ли артикул из склада в нашем
        matches = df_stock[df_stock['Код_норм'].apply(lambda x: normalized_article in x if isinstance(x, str) else False)]
        if len(matches) == 1:
            found_code = matches.iloc[0]['Код']
            logger.info(f"      ✅ Найден по обратному частичному совпадению: '{original_article}' -> '{found_code}'")
            return matches
    
    logger.warning(f"      ❌ НЕ НАЙДЕН: '{original_article}'")
    return pd.DataFrame()

# ============================================
# 5. ОСТАЛЬНЫЕ ФУНКЦИИ (БЕЗ ИЗМЕНЕНИЙ)
# ============================================

def download_file(url, filename):
    """Скачивает файл по указанной ссылке"""
    logger.info(f"📥 Попытка скачать файл {filename}...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*',
        }
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            logger.info(f"   ✅ Файл успешно скачан: {filename} ({len(response.content)} байт)")
            return True
        else:
            logger.error(f"   ❌ Ошибка при скачивании {filename}: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"   ❌ Ошибка при скачивании {filename}: {e}")
        return False

def load_stock_file(filename):
    """Загружает один файл склада и возвращает DataFrame"""
    try:
        df = pd.read_excel(filename, sheet_name=0, header=0)
        expected_columns = ['Код', 'Бренд', 'Наименование', 'Цена', 'ID_поставщика', 'Наличие', 'Срок']
        
        if len(df.columns) != 7:
            logger.warning(f"   ⚠️ Нестандартное количество колонок: {len(df.columns)}")
            df = pd.read_excel(filename, sheet_name=0, header=None, skiprows=1)
            if len(df.columns) >= 7:
                df = df.iloc[:, :7]
                df.columns = expected_columns
            else:
                logger.error(f"   ❌ Не удалось определить структуру файла")
                return pd.DataFrame()
        else:
            df.columns = expected_columns
            
        df = df.dropna(subset=['Код']).copy()
        df['Код'] = df['Код'].astype(str).str.strip()
        
        df['Цена'] = df['Цена'].astype(str).str.replace(',', '.').str.replace(' ', '')
        df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce')
        df['Наличие'] = pd.to_numeric(df['Наличие'], errors='coerce').fillna(0)
        df['Срок'] = pd.to_numeric(df['Срок'], errors='coerce').fillna(999)
        
        logger.info(f"   ✅ Загружено {len(df)} строк из {filename}")
        return df
    except Exception as e:
        logger.error(f"   ❌ Ошибка при чтении файла {filename}: {e}")
        return pd.DataFrame()

def clean_kit_name(full_name):
    """Очищает название комплекта"""
    if not isinstance(full_name, str):
        return full_name
    name = full_name.strip()
    if ' /' in name:
        return name.rsplit(' /', 1)[0].strip()
    if name.endswith('/'):
        return name[:-1].strip()
    return name

def parse_all_kits_from_file(filename):
    """Парсит файл с комплектами"""
    logger.info(f"📋 Загрузка комплектов из файла {filename}...")
    
    try:
        df = pd.read_excel(filename, sheet_name=0, header=None)
        logger.info(f"   Всего строк в файле: {len(df)}")

        kits = {}
        current_kit = None
        kit_components = []
        kit_name = ""
        kit_article = ""

        i = 0
        while i < len(df):
            row = df.iloc[i].astype(str).tolist()
            
            if len(row) > 1 and row[1] == 'Комплект':
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
                                    if comp not in seen and comp not in ['nan', 'Артикул']:
                                        seen.add(comp)
                                        unique_components.append(comp)
                                
                                if len(unique_components) > 0:
                                    clean_name = clean_kit_name(kit_name)
                                    kits[kit_article] = {
                                        'name': clean_name,
                                        'components': unique_components
                                    }
                                    logger.info(f"      ✅ Загружен комплект {kit_article}: {len(unique_components)} компонентов")
                            
                            kit_name = potential_name
                            kit_article = potential_article
                            kit_components = []
                            current_kit = kit_article
                            i += 2
                            continue
            
            if current_kit and len(row) > 2:
                article = str(row[2]).strip()
                if (article and article != 'nan' and article != 'Артикул' and
                    not article.startswith('УТ') and len(article) > 1 and len(article) < 30):
                    exclude_words = ['гофроящик', 'этикетка', 'ложемент', 'наименование',
                                   'комплект', 'бренд', 'код', 'упаковка', 'коробка']
                    article_lower = article.lower()
                    if not any(word in article_lower for word in exclude_words):
                        kit_components.append(article)
            
            i += 1

        if current_kit and len(kit_components) > 0:
            unique_components = []
            seen = set()
            for comp in kit_components:
                if comp not in seen and comp not in ['nan', 'Артикул']:
                    seen.add(comp)
                    unique_components.append(comp)
            
            if len(unique_components) > 0:
                clean_name = clean_kit_name(kit_name)
                kits[kit_article] = {
                    'name': clean_name,
                    'components': unique_components
                }
                logger.info(f"      ✅ Загружен комплект {kit_article}: {len(unique_components)} компонентов")

        logger.info(f"\n   ✅ Всего загружено комплектов: {len(kits)}")
        return kits
    except Exception as e:
        logger.error(f"   ❌ Ошибка при загрузке файла: {e}")
        return {}

def calculate_max_quantity_with_groups(components, df_stock, kit_article):
    """Рассчитать максимальное количество комплектов"""
    if df_stock.empty:
        return 0, [], None, None

    available_items = {}
    missing_articles = []

    logger.info(f"      Поиск компонентов для {kit_article} (всего {len(components)}):")
    
    for article in components:
        items = find_stock_items(article, df_stock)

        if items.empty:
            missing_articles.append(article)
            continue

        available = items[items['Наличие'] > 0].copy()

        if available.empty:
            missing_articles.append(article)
            continue

        available = available[pd.notna(available['Цена'])]
        if available.empty:
            missing_articles.append(article)
            continue

        available = available.sort_values(['Срок', 'Цена'])
        available_items[article] = available.to_dict('records')

    if missing_articles:
        if len(missing_articles) < 10:
            logger.warning(f"      ⚠️ Отсутствуют: {missing_articles}")
        else:
            logger.warning(f"      ⚠️ Отсутствуют {len(missing_articles)} компонентов, первые 10: {missing_articles[:10]}")
        return 0, [], missing_articles[0] if missing_articles else None, 0

    limiting_article = None
    limiting_qty = float('inf')
    
    for article, items in available_items.items():
        total_qty = sum(item['Наличие'] for item in items)
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
                'source': f"{item.get('ID_поставщика', '?')}",
                'price': item['Цена'],
                'delivery': item['Срок'],
                'qty': item['Наличие']
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
# 6. ОСНОВНАЯ ФУНКЦИЯ
# ============================================

def main():
    """Основная функция анализа"""
    logger.info("="*70)
    logger.info("🚀 ЗАПУСК АНАЛИЗА СКЛАДСКИХ ОСТАТКОВ")
    logger.info(f"📅 Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    # Загрузка файлов склада
    logger.info("📦 Загрузка файлов складских остатков...")
    all_stock_dfs = []
    
    for i, (url, filename) in enumerate(zip(STOCK_FILE_URLS, STOCK_FILENAMES)):
        logger.info(f"\n📄 Файл {i+1}: {filename}")
        
        if os.path.exists(filename):
            logger.info(f"   ✅ Файл {filename} уже существует")
            file_exists = True
        else:
            file_exists = download_file(url, filename)
        
        if file_exists:
            df = load_stock_file(filename)
            if not df.empty:
                all_stock_dfs.append(df)
    
    if all_stock_dfs:
        df_stock = pd.concat(all_stock_dfs, ignore_index=True)
        df_stock = df_stock.drop_duplicates(subset=['Код', 'ID_поставщика', 'Цена'], keep='first')
        logger.info(f"\n✅ Всего загружено: {len(df_stock)} строк из {len(all_stock_dfs)} файлов")
    else:
        logger.error("⚠️ Не удалось загрузить ни одного файла склада")
        df_stock = pd.DataFrame(columns=['Код', 'Цена', 'Наличие', 'Срок', 'ID_поставщика'])
    
    # Загрузка комплектов
    kits_file = 'vse_lozhementy.xlsx'
    if not os.path.exists(kits_file):
        for filename in STOCK_FILENAMES:
            if 'lozhement' in filename.lower() or 'ложемент' in filename.lower():
                kits_file = filename
                break
    
    if not os.path.exists(kits_file):
        logger.error(f"❌ Файл с комплектами не найден!")
        return
    
    kits = parse_all_kits_from_file(kits_file)
    
    if not kits:
        logger.error("❌ Нет загруженных комплектов для анализа!")
        return
    
    # Анализ
    logger.info("\n🔍 АНАЛИЗ КОМПЛЕКТОВ")
    logger.info("="*70)
    
    all_results = []
    
    for kit_article, kit_info in kits.items():
        logger.info(f"\n▶️ Анализ {kit_article} ({kit_info['name']})...")
        
        max_qty, groups, limiting_art, limiting_qty = calculate_max_quantity_with_groups(
            kit_info['components'], df_stock, kit_article
        )
        
        # Заголовок
        all_results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
        all_results.append({
            'Комплект': '',
            'Артикул': '',
            'Бренд': '',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
        
        # Результаты
        if max_qty > 0 and groups:
            for group in groups:
                all_results.append({
                    'Комплект': kit_info['name'],
                    'Артикул': kit_article,
                    'Бренд': 'PowerMechanics',
                    'Количество': group['count'],
                    'Цена': f"{group['price']:.2f} ₽",
                    'Срок': str(group['delivery'])
                })
            
            all_results.append({
                'Комплект': 'Всего комплектов по наличию:',
                'Артикул': '',
                'Бренд': '',
                'Количество': max_qty,
                'Цена': '',
                'Срок': ''
            })
            if limiting_art:
                all_results.append({
                    'Комплект': 'Лимитирующий компонент:',
                    'Артикул': limiting_art,
                    'Бренд': '',
                    'Количество': limiting_qty,
                    'Цена': '',
                    'Срок': ''
                })
        else:
            all_results.append({
                'Комплект': kit_info['name'],
                'Артикул': kit_article,
                'Бренд': 'PowerMechanics',
                'Количество': 0,
                'Цена': '—',
                'Срок': '—'
            })
        
        all_results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
    
    # Сохранение результатов
    output_filename = f'results_{datetime.now().strftime("%Y%m%d")}.csv'
    df_results = pd.DataFrame(all_results)
    df_results.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    logger.info(f"\n💾 Результаты сохранены в файл: {output_filename}")
    logger.info(f"📊 Проанализировано комплектов: {len(kits)}")
    logger.info("✨ Анализ завершен успешно!")
    
    # Метаданные
    metadata = {
        'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'kits_analyzed': len(kits),
        'stock_rows': len(df_stock) if not df_stock.empty else 0,
        'output_file': output_filename
    }
    
    with open('metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
