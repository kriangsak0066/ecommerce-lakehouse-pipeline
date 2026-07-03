import json
import websocket
from kafka import KafkaProducer

# 1. ตั้งค่า Kafka Producer
# สร้างตัวส่งข้อมูลไปที่ Kafka ที่เรารันไว้ใน Docker (Port 9092)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # แปลงข้อมูลเป็น JSON ก่อนส่ง
    api_version=(2, 8, 1) # แก้ปัญหา UnrecognizedBrokerVersion
)

TOPIC_NAME = 'crypto_trades'

# 2. ฟังก์ชันนี้จะถูกเรียกทุกครั้งที่มีข้อมูลใหม่เด้งมาจาก Binance
def on_message(ws, message):
    try:
        # แปลงข้อความจาก String เป็น Dictionary (JSON)
        raw_data = json.loads(message)
        
        # ถ้ารับข้อมูลแบบ Multi-stream ตัวแปรจะซ้อนอยู่ในคีย์ 'data'
        trade_data = raw_data.get('data', raw_data)
        
        # จัดฟอร์แมตข้อมูลใหม่ให้ดูง่ายขึ้น
        cleaned_data = {
            "symbol": trade_data.get('s'),           # ชื่อเหรียญ เช่น BTCUSDT
            "price": float(trade_data.get('p', 0)),  # ราคาที่ตกลงซื้อขาย
            "quantity": float(trade_data.get('q', 0)), # จำนวนเหรียญ
            "trade_time": trade_data.get('T'),       # Timestamp (Epoch time)
            "is_buyer_maker": trade_data.get('m')    # ฝั่งซื้อหรือขายเป็นคนตั้งออเดอร์
        }
        
        # ส่งข้อมูลเข้า Kafka Topic
        producer.send(TOPIC_NAME, value=cleaned_data)
        
        print(f"[OK] Sent to Kafka -> Symbol: {cleaned_data['symbol']} | Price: {cleaned_data['price']:.2f} | Qty: {cleaned_data['quantity']}")
        
    except Exception as e:
        print(f"[ERROR] Error processing message: {e}")

def on_error(ws, error):
    print(f"[WARN] WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("[STOP] WebSocket Connection Closed")
    producer.close()

def on_open(ws):
    print("[START] Connected to Binance WebSocket!")
    print(f"[INFO] Streaming live trades into Kafka Topic: {TOPIC_NAME}...")

if __name__ == "__main__":
    # 3. กำหนด URL ของ Binance WebSocket (ดึงข้อมูล Trade ของ 3 เหรียญหลัก: BTC, ETH, BNB)
    SOCKET = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade"
    
    # เริ่มการเชื่อมต่อ
    ws = websocket.WebSocketApp(
        SOCKET,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # สั่งให้รันวนลูปไปเรื่อยๆ ไม่มีที่สิ้นสุด
    ws.run_forever()
