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
# 3. ФУНКЦИИ
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
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type and 'google' not in url:
                logger.warning(f"   ⚠️ Сервер вернул HTML-страницу для {filename}")
                return False
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
    """Очищает название комплекта, удаляя всё после последнего слеша"""
    if not isinstance(full_name, str):
        return full_name
    name = full_name.strip()
    # Удаляем /JTC/ и подобное в конце
    if ' /' in name:
        return name.rsplit(' /', 1)[0].strip()
    if name.endswith('/'):
        return name[:-1].strip()
    return name

def find_stock_items(article, df_stock):
    """Поиск артикулов с нормализацией"""
    if df_stock.empty:
        return pd.DataFrame()
    
    article_upper = article.upper().strip()
    
    result = df_stock[df_stock['Код'].str.upper() == article_upper]
    if not result.empty:
        return result

    normalized = article_upper.replace('-', '')
    result = df_stock[df_stock['Код'].str.upper() == normalized]
    if not result.empty:
        logger.debug(f"      🔍 Найден {article} как {normalized}")
        return result

    return pd.DataFrame()

def parse_all_kits_from_file(filename):
    """
    Парсит файл с комплектами в формате:
    Строка 1: (пусто) | Комплект | Артикул | Код
    Строка 2: (пусто) | Название комплекта | Артикул комплекта | ...
    Строка 3: пустая
    Строка 4: (пусто) | Наименование | Артикул | Бренд
    Строка 5+: компоненты
    """
    logger.info(f"📋 Загрузка комплектов из файла {filename}...")

    try:
        # Читаем весь файл без заголовков
        df = pd.read_excel(filename, sheet_name=0, header=None)
        logger.info(f"   Всего строк в файле: {len(df)}")

        kits = {}
        i = 0
        
        while i < len(df):
            row = df.iloc[i].astype(str).tolist()
            
            # Ищем строку с "Комплект" в колонке B (индекс 1)
            if len(row) > 1 and row[1] == 'Комплект':
                # Следующая строка содержит название и артикул комплекта
                if i + 1 < len(df):
                    kit_row = df.iloc[i + 1].astype(str).tolist()
                    
                    # Название в колонке B (индекс 1), артикул в колонке C (индекс 2)
                    kit_name = kit_row[1] if len(kit_row) > 1 else ""
                    kit_article = kit_row[2] if len(kit_row) > 2 else ""
                    
                    # Очищаем название от " /JTC/" и подобного
                    kit_name = clean_kit_name(kit_name)
                    
                    logger.info(f"   Найден комплект: {kit_article} - {kit_name}")
                    
                    # Ищем компоненты (после строки с "Наименование")
                    # Строка с "Наименование" обычно на i+3 (через одну пустую)
                    components = []
                    
                    # Находим строку с "Наименование" в колонке B
                    j = i + 2
                    while j < len(df):
                        check_row = df.iloc[j].astype(str).tolist()
                        if len(check_row) > 1 and check_row[1] == 'Наименование':
                            j += 1  # Переходим к следующей строке после заголовка
                            break
                        j += 1
                    
                    # Собираем компоненты
                    while j < len(df):
                        comp_row = df.iloc[j].astype(str).tolist()
                        
                        # Проверяем, не начался ли следующий комплект
                        if len(comp_row) > 1 and comp_row[1] == 'Комплект':
                            break
                        
                        # Проверяем пустую строку (конец комплекта)
                        if len(comp_row) > 1 and comp_row[1] in ['', 'nan'] and len(comp_row) > 2 and comp_row[2] in ['', 'nan']:
                            # Пустая строка — возможно конец комплекта
                            # Проверим следующую строку
                            if j + 1 < len(df):
                                next_row = df.iloc[j + 1].astype(str).tolist()
                                if len(next_row) > 1 and next_row[1] == 'Комплект':
                                    break
                        
                        # Проверяем, что это валидный артикул компонента
                        if len(comp_row) > 2 and comp_row[2] and comp_row[2] != 'nan' and comp_row[2] != '':
                            article = comp_row[2].strip()
                            
                            # Пропускаем служебные слова
                            exclude_words = ['гофроящик', 'этикетка', 'ложемент', 'наименование',
                                           'комплект', 'бренд', 'код', 'упаковка', 'коробка',
                                           'Наименование', 'Артикул', 'Бренд', 'В комплекте',
                                           'Остаток', 'Цена']
                            
                            if article not in exclude_words and len(article) > 2 and not article.startswith('УТ'):
                                components.append(article)
                        
                        j += 1
                    
                    if components:
                        # Удаляем дубликаты
                        unique_components = []
                        seen = set()
                        for comp in components:
                            if comp not in seen:
                                seen.add(comp)
                                unique_components.append(comp)
                        
                        kits[kit_article] = {
                            'name': kit_name,
                            'components': unique_components
                        }
                        logger.info(f"      ✅ Загружен комплект {kit_article}: {len(unique_components)} компонентов")
                    
                    i = j  # Переходим к следующему комплекту
                    continue
            
            i += 1
        
        logger.info(f"\n   ✅ Всего загружено комплектов: {len(kits)}")
        return kits
        
    except Exception as e:
        logger.error(f"   ❌ Ошибка при загрузке файла: {e}")
        import traceback
        traceback.print_exc()
        return {}

def calculate_max_quantity_with_groups(components, df_stock, kit_article):
    """Рассчитать максимальное количество комплектов"""
    if df_stock.empty:
        return 0, [], None, None

    available_items = {}
    missing_articles = []

    logger.info(f"      Поиск компонентов для {kit_article} (всего {len(components)}):")
    
    # Показываем первые 5 компонентов для отладки
    for article in components[:5]:
        items = find_stock_items(article, df_stock)
        if not items.empty:
            logger.info(f"        ✅ {article} -> найден")
        else:
            logger.info(f"        ❌ {article} -> НЕ НАЙДЕН")
    
    # Полный поиск всех компонентов
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
        if len(missing_articles) < 5:
            logger.warning(f"      ⚠️ Отсутствуют: {missing_articles}")
        else:
            logger.warning(f"      ⚠️ Отсутствуют {len(missing_articles)} компонентов, первые 5: {missing_articles[:5]}")
        return 0, [], missing_articles[0] if missing_articles else None, 0

    # Определяем лимитирующий компонент
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

    # Создаём копии остатков
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

    # Формируем комплекты
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
    
    # Группируем одинаковые комплекты
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
# 4. ОСНОВНАЯ ФУНКЦИЯ
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
        logger.info("Проверьте структуру файла vse_lozhementy.xlsx")
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
        
        # Заголовок комплекта
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
        
        # Срочная поставка (заглушка)
        all_results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
        
        # Минимальная цена (заглушка)
        all_results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
        
        # Результаты по наличию
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
        
        # Разделитель между комплектами
        all_results.append({
            'Комплект': '',
            'Артикул': '',
            'Бренд': '',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
    
    # Сохранение результатов
    output_filename = f'results_{datetime.now().strftime("%Y%m%d")}.csv'
    df_results = pd.DataFrame(all_results)
    df_results.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    logger.info(f"\n💾 Результаты сохранены в файл: {output_filename}")
    logger.info(f"📊 Проанализировано комплектов: {len(kits)}")
    logger.info("✨ Анализ завершен успешно!")
    
    # Создаём файл с метаданными
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
