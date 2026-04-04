"""
LAB 04: Демонстрация проблемы retry без идемпотентности.

Сценарий:
1) Клиент отправил запрос на оплату.
2) До получения ответа \"сеть оборвалась\" (моделируем повтором запроса).
3) Клиент повторил запрос БЕЗ Idempotency-Key.
4) В unsafe-режиме возможна двойная оплата.
"""

import pytest
import uuid
import httpx
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from app.main import app

import os
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@db:5432/marketplace")

@pytest.fixture
async def test_order():
    engine = create_async_engine(DATABASE_URL)
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()

    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO users (id, email, name) VALUES (:id, :email, :name)"),
            {"id": str(user_id), "email": f"test_{user_id}@test.com", "name": "Test User"}
        )
        await session.execute(
            text("INSERT INTO orders (id, user_id, status, total_amount) VALUES (:id, :user_id, 'created', 100.0)"),
            {"id": str(order_id), "user_id": str(user_id)}
        )
        await session.commit()

    yield order_id

    async with AsyncSession(engine) as session:
        await session.execute(text("DELETE FROM order_status_history WHERE order_id = :id"), {"id": str(order_id)})
        await session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": str(order_id)})
        await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": str(user_id)})
        await session.commit()

    await engine.dispose()


@pytest.mark.asyncio
async def test_retry_without_idempotency_can_double_pay(test_order):
    """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Выполнить две параллельные попытки POST /api/payments/retry-demo
       с mode='unsafe' и БЕЗ заголовка Idempotency-Key.
    3) Проверить историю order_status_history:
       - paid-событий больше 1 (или иная метрика двойного списания).
    4) Вывести понятный отчёт в stdout:
       - сколько попыток
       - сколько paid в истории
       - почему это проблема.
    """
    order_id = test_order

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test"
    ) as client:

        response1 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_id), "mode": "unsafe"}
        )

        response2 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_id), "mode": "unsafe"}
        )

    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response1.json()["success"] is True
    assert response2.json()["success"] is False
    assert "X-Idempotency-Replayed" not in response2.headers

    print("\nRETRY БЕЗ IDEMPOTENCY KEY")
    print(f"Первый запрос:  {response1.status_code} success={response1.json()['success']} {response1.json()['message']}")
    print(f"Второй запрос:  {response2.status_code} success={response2.json()['success']} {response2.json()['message']}")
    print("Проблема: клиент не может отличить 'оплата прошла' от 'ошибка при повторе'.")
    print("Оба запроса вернули HTTP 200, но с разным success. Без ключа клиент в растерянности.")
