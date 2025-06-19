import json
import time
import logging
from collections import defaultdict

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, PyMongoError

# Константы подключения
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICS = [
    'postgres_server.public.university',
    'postgres_server.public.institute',
    'postgres_server.public.department',
    'postgres_server.public.specialty'
]
GROUP_ID = 'mongo-sync-consumer-group'

MONGO_URI = 'mongodb://admin:secret@localhost:27017/'
MONGO_DB = 'university_db'
COLLECTION = 'universities'

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('kafka_mongo_sync')

def setup_mongo():
    """
    Сбрасывает и создаёт коллекцию в MongoDB с JSON Schema валидацией
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    try:
        db.drop_collection(COLLECTION)
        logger.info(f"Dropped existing collection '{COLLECTION}'")
    except Exception as e:
        logger.warning(f"Could not drop collection '{COLLECTION}': {e}")

    validator = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['name', 'location', 'institutes'],
            'properties': {
                'name': {'bsonType': 'string'},
                'location': {'bsonType': 'string'},
                'institutes': {
                    'bsonType': 'array',
                    'items': {
                        'bsonType': 'object',
                        'required': ['name', 'departments'],
                        'properties': {
                            'name': {'bsonType': 'string'},
                            'departments': {
                                'bsonType': 'array',
                                'items': {
                                    'bsonType': 'object',
                                    'required': ['name'],
                                    'properties': {
                                        'name': {'bsonType': 'string'},
                                        'specializations': {
                                            'bsonType': 'array',
                                            'items': {'bsonType': 'string'}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    try:
        db.create_collection(COLLECTION, validator={'$jsonSchema': validator['$jsonSchema']})
        logger.info(f"Created collection '{COLLECTION}' with schema validation")
    except CollectionInvalid:
        logger.error(f"Collection '{COLLECTION}' already exists or invalid schema")
        raise
    except PyMongoError as e:
        logger.error(f"Failed to create collection '{COLLECTION}': {e}")
        raise

    return client, db[COLLECTION]

def consume_snapshot(batch_timeout=1000, max_idle_ms=5000):
    """
    Читает snapshot + CDC-сообщения из Kafka и агрегирует данные
    batch_timeout: время polling в миллисекундах
    max_idle_ms: прекращает чтение, если нет сообщений за этот период (мс)
    """
    logger.info(f"Starting Kafka consumer for topics: {TOPICS}, group_id={GROUP_ID}")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        max_poll_records=500,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    unis, insts, depts, specs = {}, {}, {}, defaultdict(list)
    last_received = time.time()
    total_msgs = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=batch_timeout)
            batch_count = sum(len(v) for v in records.values())
            if batch_count:
                total_msgs += batch_count
                logger.info(f"Polled {batch_count} messages (total {total_msgs})")
                last_received = time.time()
                for tp, msgs in records.items():
                    for msg in msgs:
                        # Определяем реальное тело CDC-события: может быть обёрнуто в payload
                        raw = msg.value
                        if isinstance(raw, dict) and 'payload' in raw:
                            payload = raw['payload']
                        else:
                            payload = raw
                        # Для отладки: логируем ключи первого сообщения
                        if total_msgs == 0:
                            logger.debug(f"First message keys: {list(raw.keys())}, payload keys: {list(payload.keys())}")
                        op = payload.get('op')
                        after = payload.get('after')
                        before = payload.get('before')
                        data = after or before
                        if not data:
                            continue
                        topic = tp.topic
                        logger.debug(f"Msg {tp.topic}:{tp.partition}@{msg.offset} op={op} data_keys={list(data.keys())}")
                        # сущности
                        if topic.endswith('university'):
                            uid = data.get('id') or data.get('Id') or data.get('ID')
                            if uid is None:
                                logger.warning(f"University record without id field: {data}")
                                continue
                            if op in ('c', 'r', 'u', 'create'):
                                unis[uid] = {'name': data.get('name'), 'location': data.get('location')}
                                logger.debug(f"University {uid} set: {unis[uid]}")
                            elif op == 'd':
                                unis.pop(uid, None)
                                logger.debug(f"University {uid} removed")
                        elif topic.endswith('institute'):
                            iid = data.get('id') or data.get('institute_id')
                            if iid is None:
                                logger.warning(f"Institute record without id field: {data}")
                                continue
                            if op in ('c', 'r', 'u', 'create'):
                                insts[iid] = {'name': data.get('name'), 'university_id': data.get('university_id')}
                            elif op == 'd':
                                insts.pop(iid, None)
                        elif topic.endswith('department'):
                            did = data.get('id') or data.get('department_id')
                            if did is None:
                                logger.warning(f"Department record without id field: {data}")
                                continue
                            if op in ('c', 'r', 'u', 'create'):
                                depts[did] = {'name': data.get('name'), 'institute_id': data.get('institute_id')}
                            elif op == 'd':
                                depts.pop(did, None)
                                specs.pop(did, None)
                        elif topic.endswith('specialty'):
                            dept_id = data.get('department_id')
                            name = data.get('name')
                            if dept_id is None or name is None:
                                logger.warning(f"Specialty record missing fields: {data}")
                                continue
                            if op in ('c', 'r', 'u', 'create'):
                                if name not in specs[dept_id]:
                                    specs[dept_id].append(name)
                            elif op == 'd':
                                specs[dept_id] = [n for n in specs[dept_id] if n != name]
                    for msg in msgs:
                        payload = msg.value.get('payload', {})
                        op = payload.get('op')
                        after = payload.get('after')
                        before = payload.get('before')
                        data = after or before
                        if not data:
                            continue
                        topic = tp.topic
                        logger.debug(f"Msg {tp.topic}:{tp.partition}@{msg.offset} op={op} data={data}")
                        # сущности
                        if topic.endswith('university'):
                            uid = data['id']
                            if op in ('c', 'r', 'u'):
                                unis[uid] = {'name': data['name'], 'location': data['location']}
                                logger.debug(f"University {uid} set: {unis[uid]}")
                            elif op == 'd':
                                unis.pop(uid, None)
                                logger.debug(f"University {uid} removed")
                        elif topic.endswith('institute'):
                            iid = data['id']
                            if op in ('c', 'r', 'u'):
                                insts[iid] = {'name': data['name'], 'university_id': data['university_id']}
                            elif op == 'd':
                                insts.pop(iid, None)
                        elif topic.endswith('department'):
                            did = data['id']
                            if op in ('c', 'r', 'u'):
                                depts[did] = {'name': data['name'], 'institute_id': data['institute_id']}
                            elif op == 'd':
                                depts.pop(did, None)
                                specs.pop(did, None)
                        elif topic.endswith('specialty'):
                            dept_id = data['department_id']
                            name = data['name']
                            if op in ('c', 'r', 'u'):
                                if name not in specs[dept_id]:
                                    specs[dept_id].append(name)
                            elif op == 'd':
                                specs[dept_id] = [n for n in specs[dept_id] if n != name]
            else:
                idle = (time.time() - last_received) * 1000
                if idle > max_idle_ms:
                    logger.info(f"No messages for {max_idle_ms}ms (idle {int(idle)}ms), stopping consumption")
                    break
    except Exception as e:
        logger.error(f"Error consuming Kafka: {e}", exc_info=True)
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")
    logger.info(f"Finished consumption: total messages={total_msgs}, universities={len(unis)}, institutes={len(insts)}, departments={len(depts)}, specialties entries={sum(len(v) for v in specs.values())}")
    return unis, insts, depts, specs

def build_and_insert_docs(collection, unis, insts, depts, specs):
    docs = []
    for uid, uni in unis.items():
        inst_list = []
        for iid, inst in insts.items():
            if inst['university_id'] != uid:
                continue
            dept_list = []
            for did, dept in depts.items():
                if dept['institute_id'] != iid:
                    continue
                dept_list.append({'name': dept['name'], 'specializations': specs.get(did, [])})
            inst_list.append({'name': inst['name'], 'departments': dept_list})
        docs.append({'name': uni['name'], 'location': uni['location'], 'institutes': inst_list})

    if not docs:
        logger.warning("No documents to insert into MongoDB")
        return
    try:
        res = collection.insert_many(docs)
        logger.info(f"Inserted {len(res.inserted_ids)} docs into MongoDB")
    except PyMongoError as e:
        logger.error(f"Failed to insert docs: {e}", exc_info=True)

def main():
    logger.info("Starting MongoDB-Kafka sync job")
    client, col = setup_mongo()
    unis, insts, depts, specs = consume_snapshot()
    build_and_insert_docs(col, unis, insts, depts, specs)
    client.close()
    logger.info("Job completed and MongoDB connection closed")

if __name__ == '__main__':
    main()
