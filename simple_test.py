#!/usr/bin/env python3
"""
Simple test script to check if your pipeline components are working
"""

def test_imports():
    """Test if all your modules can be imported"""
    print("🧪 Testing imports...")
    
    try:
        from config.settings import get_settings
        print("✅ config.settings - OK")
    except Exception as e:
        print(f"❌ config.settings - Error: {e}")
    
    try:
        from utils.logger import get_logger
        print("✅ utils.logger - OK")
    except Exception as e:
        print(f"❌ utils.logger - Error: {e}")
    
    try:
        from kafka.topics import create_topics
        print("✅ kafka.topics - OK")
    except Exception as e:
        print(f"❌ kafka.topics - Error: {e}")
    
    try:
        from kafka.kafka_producer import KafkaProducerService
        print("✅ kafka.kafka_producer - OK")
    except Exception as e:
        print(f"❌ kafka.kafka_producer - Error: {e}")
    
    try:
        from kafka.kafka_consumer import KafkaConsumerService
        print("✅ kafka.kafka_consumer - OK")
    except Exception as e:
        print(f"❌ kafka.kafka_consumer - Error: {e}")
    
    try:
        from alerts.alert_logic import AlertEngine
        print("✅ alerts.alert_logic - OK")
    except Exception as e:
        print(f"❌ alerts.alert_logic - Error: {e}")
    
    try:
        from alerts.alert_store import AlertStore
        print("✅ alerts.alert_store - OK")
    except Exception as e:
        print(f"❌ alerts.alert_store - Error: {e}")
    
    try:
        from ws.websocket_manager import WebSocketManager
        print("✅ ws.websocket_manager - OK")
    except Exception as e:
        print(f"❌ ws.websocket_manager - Error: {e}")
    
    try:
        from api.auth import AuthService
        print("✅ api.auth - OK")
    except Exception as e:
        print(f"❌ api.auth - Error: {e}")

def test_basic_functionality():
    """Test basic functionality without Kafka running"""
    print("\n🔧 Testing basic functionality...")
    
    try:
        # Test settings
        from config.settings import get_settings
        settings = get_settings()
        print(f"✅ Settings loaded - Environment: {settings.environment}")
    except Exception as e:
        print(f"❌ Settings test failed: {e}")
    
    try:
        # Test logger
        from utils.logger import get_logger
        logger = get_logger("test")
        logger.info("Test log message")
        print("✅ Logger working")
    except Exception as e:
        print(f"❌ Logger test failed: {e}")
    
    try:
        # Test alert schema
        from alerts.alert_schema import Alert
        alert = Alert(
            alert_type="intrusion",
            message="Test alert",
            camera_id="cam_001",
            timestamp="2025-07-17T10:30:00Z"
        )
        print(f"✅ Alert schema working - Created: {alert.alert_type}")
    except Exception as e:
        print(f"❌ Alert schema test failed: {e}")
    
    try:
        # Test alert store
        from alerts.alert_store import AlertStore
        store = AlertStore()
        print("✅ Alert store created")
    except Exception as e:
        print(f"❌ Alert store test failed: {e}")

if __name__ == "__main__":
    print("🚀 Testing Your Pipeline Components\n")
    test_imports()
    test_basic_functionality()
    print("\n🎉 Test completed!")