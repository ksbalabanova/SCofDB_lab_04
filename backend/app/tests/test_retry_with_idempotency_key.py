"""
LAB 04: Проверка идемпотентного повтора запроса.

Цель:
При повторном запросе с тем же Idempotency-Key вернуть
кэшированный результат без повторного списания.
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
        await session.execute(text("DELETE FROM idempotency_keys WHERE idempotency_key LIKE :pattern"), {"pattern": f"%{order_id}%"})
        await session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": str(order_id)})
        await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": str(user_id)})
        await session.commit()

    await engine.dispose()


@pytest.mark.asyncio
async def test_retry_with_same_key_returns_cached_response(test_order):
    """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Сделать первый POST /api/payments/retry-demo (mode='unsafe')
       с заголовком Idempotency-Key: fixed-key-123.
    3) Повторить тот же POST с тем же ключом и тем же payload.
    4) Проверить:
       - второй ответ пришёл из кэша (через признак, который вы добавите,
         например header X-Idempotency-Replayed=true),
       - в order_status_history только одно событие paid,
       - в idempotency_keys есть запись completed с response_body/status_code.
    """
    order_id = test_order
    idempotency_key = f"test-key-{order_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test"
    ) as client:
        response1 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_id), "mode": "unsafe"},
            headers={"Idempotency-Key": idempotency_key}
        )
        response2 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_id), "mode": "unsafe"},
            headers={"Idempotency-Key": idempotency_key}
        )
    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response2.headers.get("X-Idempotency-Replayed") == "true"
    assert response1.json() == response2.json()
    engine = create_async_engine(DATABASE_URL)
    
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM order_status_history WHERE order_id = :id AND status = 'paid'"),
            {"id": str(order_id)}
        )
        paid_count = result.scalar()
        result2 = await session.execute(
            text("SELECT status, status_code, response_body FROM idempotency_keys WHERE idempotency_key = :key"),
            {"key": idempotency_key}
        )
        record = result2.mappings().first()
    await engine.dispose()

    assert paid_count == 1, f"Ожидалась 1 запись об оплате, получили {paid_count}"
    assert record is not None, "Запись в idempotency_keys не найдена"
    assert record["status"] == "completed"
    assert record["status_code"] == 200
    assert record["response_body"] is not None

    print("\nRETRY С IDEMPOTENCY KEY")
    print(f"Первый запрос:  {response1.status_code} {response1.json()}")
    print(f"Второй запрос:  {response2.status_code} {response2.json()}")
    print(f"X-Idempotency-Replayed: {response2.headers.get('X-Idempotency-Replayed')}")
    print(f"Записей об оплате в БД: {paid_count} (двойного списания нет)")
    print(f"Запись в idempotency_keys: status={record['status']}, status_code={record['status_code']}")


@pytest.mark.asyncio
async def test_same_key_different_payload_returns_conflict(test_order):
    """
    TODO: Реализовать негативный тест.

    Один и тот же Idempotency-Key нельзя использовать с другим payload.
    Ожидается 409 Conflict (или эквивалентная бизнес-ошибка).
    """
    order_id = test_order
    idempotency_key = f"conflict-key-{order_id}"

    engine = create_async_engine(DATABASE_URL)
    other_order_id = uuid.uuid4()
    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO orders (id, user_id, status, total_amount) SELECT :id, user_id, 'created', 100.0 FROM orders WHERE id = :orig"),
            {"id": str(other_order_id), "orig": str(order_id)}
        )
        await session.commit()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test"
    ) as client:

        response1 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_id), "mode": "unsafe"},
            headers={"Idempotency-Key": idempotency_key}
        )

        response2 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(other_order_id), "mode": "unsafe"},
            headers={"Idempotency-Key": idempotency_key}
        )
        
    async with AsyncSession(engine) as session:
        await session.execute(text("DELETE FROM order_status_history WHERE order_id = :id"), {"id": str(other_order_id)})
        await session.execute(text("DELETE FROM idempotency_keys WHERE idempotency_key = :key"), {"key": idempotency_key})
        await session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": str(other_order_id)})
        await session.commit()
    await engine.dispose()

    assert response1.status_code == 200
    assert response2.status_code == 409

    print("\nПроблема")
    print(f"Первый запрос:  {response1.status_code} {response1.json()}")
    print(f"Второй запрос:  {response2.status_code} {response2.json()}")
    print("Middleware обнаружил что payload изменился и вернул 409 Conflict")
