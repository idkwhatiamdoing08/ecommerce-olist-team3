# Правила очистки (summary)

1. price <= 0 -> remove record (анализ аномалий).
2. дубликаты по (order_id, order_item_id) -> удалить.
3. статус не в {delivered, cancelled, shipped, approved} -> пометить 'unknown'.
4. даты: привести к ISO; при ошибке поставить NaT и логировать.
