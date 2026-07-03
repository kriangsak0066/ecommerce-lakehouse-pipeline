import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# ตั้งค่า Kafka Producer
# เชื่อมต่อไปที่ localhost:9092 (เปิดไว้ใน docker-compose)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'ecommerce_orders'

# รายชื่อสินค้าตัวอย่าง
PRODUCTS = [
    {"product_id": "P001", "name": "Laptop", "price": 25000},
    {"product_id": "P002", "name": "Smartphone", "price": 15000},
    {"product_id": "P003", "name": "Headphones", "price": 3000},
    {"product_id": "P004", "name": "Mechanical Keyboard", "price": 4500},
    {"product_id": "P005", "name": "Mouse", "price": 1200}
]

def generate_mock_order():
    """สุ่มสร้างข้อมูลคำสั่งซื้อ (Order)"""
    product = random.choice(PRODUCTS)
    order_id = f"ORD-{random.randint(10000, 99999)}"
    user_id = f"USR-{random.randint(100, 999)}"
    quantity = random.randint(1, 3)
    
    order_data = {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product["product_id"],
        "product_name": product["name"],
        "quantity": quantity,
        "total_amount": product["price"] * quantity,
        "order_timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return order_data

if __name__ == "__main__":
    print(f"🚀 Starting Data Generator. Sending data to Kafka Topic: {TOPIC_NAME}...")
    try:
        while True:
            # 1. สร้างข้อมูลจำลอง
            data = generate_mock_order()
            
            # 2. ส่งข้อมูลเข้า Kafka
            producer.send(TOPIC_NAME, value=data)
            
            # 3. พิมพ์แจ้งเตือนว่าส่งข้อมูลแล้ว
            print(f"Sent Order: {data['order_id']} | Amount: {data['total_amount']} THB")
            
            # หน่วงเวลา 2-5 วินาทีต่อ 1 order เพื่อให้ดูเหมือนข้อมูลค่อยๆ ไหลเข้ามาจริง
            time.sleep(random.uniform(2, 5))
            
    except KeyboardInterrupt:
        print("\n🛑 Stopped generating data.")
    finally:
        producer.close()
