"""
LAB 04: Сравнение подходов
1) FOR UPDATE (решение из lab_02)
2) Idempotency-Key + middleware (lab_04)
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
async def two_orders():
    engine = create_async_engine(DATABASE_URL)

    user_id_1 = uuid.uuid4()
    user_id_2 = uuid.uuid4()

    order_id_1 = uuid.uuid4()
    order_id_2 = uuid.uuid4() 

    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO users (id, email, name) VALUES (:id, :email, :name)"),
            {"id": str(user_id_1), "email": f"test_{user_id_1}@test.com", "name": "User 1"}
        )
        await session.execute(
            text("INSERT INTO users (id, email, name) VALUES (:id, :email, :name)"),
            {"id": str(user_id_2), "email": f"test_{user_id_2}@test.com", "name": "User 2"}
        )
        await session.execute(
            text("INSERT INTO orders (id, user_id, status, total_amount) VALUES (:id, :user_id, 'created', 100.0)"),
            {"id": str(order_id_1), "user_id": str(user_id_1)}
        )
        await session.execute(
            text("INSERT INTO orders (id, user_id, status, total_amount) VALUES (:id, :user_id, 'created', 100.0)"),
            {"id": str(order_id_2), "user_id": str(user_id_2)}
        )
        await session.commit()

    yield order_id_1, order_id_2

    async with AsyncSession(engine) as session:
        for oid in [order_id_1, order_id_2]:
            await session.execute(text("DELETE FROM order_status_history WHERE order_id = :id"), {"id": str(oid)})
            await session.execute(text("DELETE FROM idempotency_keys WHERE idempotency_key LIKE :p"), {"p": f"%{oid}%"})
            await session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": str(oid)})
        await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": str(user_id_1)})
        await session.execute(text("DELETE FROM users WHERE id = :id"), {"id": str(user_id_2)})
        await session.commit()

    await engine.dispose()


@pytest.mark.asyncio
async def test_compare_for_update_and_idempotency_behaviour(two_orders):
    """
    TODO: Реализовать сравнительный тест/сценарий.

    Минимум сравнения:
    1) Повтор запроса с mode='for_update':
       - защита от гонки на уровне БД,
       - повтор может вернуть бизнес-ошибку "already paid".
    2) Повтор запроса с mode='unsafe' + Idempotency-Key:
       - второй вызов возвращает тот же кэшированный успешный ответ,
         без повторного списания.

    В конце добавьте вывод:
    - чем отличаются цели и UX двух подходов,
    - почему они не взаимоисключающие и могут использоваться вместе.
    """
    order_for_update, order_idempotency = two_orders

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test"
    ) as client:

        fu_response1 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_for_update), "mode": "for_update"}
        )
        fu_response2 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_for_update), "mode": "for_update"}
        )

        idempotency_key = f"compare-key-{order_idempotency}"
        ik_response1 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_idempotency), "mode": "unsafe"},
            headers={"Idempotency-Key": idempotency_key}
        )
        ik_response2 = await client.post(
            "/api/payments/retry-demo",
            json={"order_id": str(order_idempotency), "mode": "unsafe"},
            headers={"Idempotency-Key": idempotency_key}
        )

    assert fu_response1.status_code == 200
    assert fu_response1.json()["success"] is True
    assert fu_response2.status_code == 200
    assert fu_response2.json()["success"] is False

    assert ik_response1.status_code == 200
    assert ik_response2.status_code == 200
    assert ik_response2.headers.get("X-Idempotency-Replayed") == "true"

    print("\nFOR UPDATE/IDEMPOTENCY KEY")
    print()
    print("FOR UPDATE (mode='for_update'):")
    print(f"Первый запрос:  {fu_response1.status_code} {fu_response1.json()}")
    print(f"Второй запрос:  {fu_response2.status_code} {fu_response2.json()}")
    print("Итог: клиент получил ошибку на retry — не знает прошла ли оплата")
    print()
    print("Idempotency-Key (mode='unsafe' + ключ):")
    print(f"Первый запрос:  {ik_response1.status_code} {ik_response1.json()}")
    print(f"Второй запрос:  {ik_response2.status_code} {ik_response2.json()}")
    print(f"X-Idempotency-Replayed: {ik_response2.headers.get('X-Idempotency-Replayed')}")
    print("Итог: клиент получил тот же успешный ответ — прозрачный retry")
    print()
    print("Вывод:")
    print("FOR UPDATE — защита от гонки на уровне БД (два запроса одновременно)")
    print("Idempotency-Key — защита от повторов на уровне API (retry после таймаута)")
    print("Они решают разные проблемы лучше использовать вместе")
