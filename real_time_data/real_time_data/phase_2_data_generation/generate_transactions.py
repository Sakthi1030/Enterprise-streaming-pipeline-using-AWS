#!/usr/bin/env python3
"""
Generate realistic sales transactions for the Sales & Marketing Platform.
Simulates real-time transaction data with seasonality, trends, and anomalies.
"""

import json
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
import boto3
import time
import hashlib
import uuid
from collections import defaultdict
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transaction_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TransactionGenerator:
    """Generate realistic sales transactions"""
    
    def __init__(self, master_data_dir: str = "data", seed: int = 42):
        self.master_data_dir = Path(master_data_dir)
        random.seed(seed)
        np.random.seed(seed)
        
        # Load master data
        self.products = self._load_master_data("products")
        self.customers = self._load_master_data("customers")
        self.stores = self._load_master_data("stores")
        self.sales_reps = self._load_master_data("sales_reps")
        self.campaigns = self._load_master_data("campaigns")
        
        # Transaction counters
        self.transaction_id_counter = 1000000
        self.order_id_counter = 500000
        
        # Time patterns
        self.time_patterns = {
            "hourly": {
                0: 0.01, 1: 0.005, 2: 0.002, 3: 0.001, 4: 0.001,
                5: 0.005, 6: 0.01, 7: 0.03, 8: 0.05, 9: 0.08,
                10: 0.10, 11: 0.12, 12: 0.15, 13: 0.14, 14: 0.12,
                15: 0.13, 16: 0.14, 17: 0.15, 18: 0.16, 19: 0.18,
                20: 0.20, 21: 0.15, 22: 0.08, 23: 0.03
            },
            "daily": {
                0: 0.18,  # Monday
                1: 0.16,  # Tuesday
                2: 0.15,  # Wednesday
                3: 0.14,  # Thursday
                4: 0.20,  # Friday
                5: 0.25,  # Saturday
                6: 0.22   # Sunday
            },
            "monthly": {
                1: 0.07, 2: 0.06, 3: 0.08, 4: 0.09, 5: 0.10,
                6: 0.11, 7: 0.10, 8: 0.09, 9: 0.08, 10: 0.09,
                11: 0.12, 12: 0.25  # December peak
            }
        }
        
        # Payment methods and success rates
        self.payment_methods = {
            "Credit Card": {"success_rate": 0.98, "processing_time": 2},
            "Debit Card": {"success_rate": 0.99, "processing_time": 1},
            "PayPal": {"success_rate": 0.97, "processing_time": 3},
            "Apple Pay": {"success_rate": 0.995, "processing_time": 1},
            "Google Pay": {"success_rate": 0.99, "processing_time": 2},
            "Bank Transfer": {"success_rate": 0.95, "processing_time": 10},
            "Cash": {"success_rate": 1.00, "processing_time": 0}
        }
        
        # Fraud patterns
        self.fraud_patterns = [
            {"type": "high_value", "threshold": 5000, "probability": 0.01},
            {"type": "multiple_cards", "threshold": 3, "probability": 0.005},
            {"type": "velocity", "threshold": 10, "probability": 0.02},
            {"type": "geo_anomaly", "probability": 0.01},
            {"type": "time_anomaly", "probability": 0.005}
        ]
        
        # Product categories with seasonal adjustments
        self.seasonal_adjustments = {
            "Winter": {"Electronics": 1.1, "Fashion": 1.3, "Sports": 0.8},
            "Spring": {"Electronics": 1.0, "Fashion": 1.2, "Sports": 1.1},
            "Summer": {"Electronics": 0.9, "Fashion": 1.1, "Sports": 1.5},
            "Fall": {"Electronics": 1.2, "Fashion": 1.4, "Sports": 1.0}
        }
        
        # Promotions and discounts
        self.promotions = [
            {"name": "Summer Sale", "discount": 0.2, "categories": ["Fashion", "Sports"]},
            {"name": "Black Friday", "discount": 0.4, "categories": ["Electronics"]},
            {"name": "Cyber Monday", "discount": 0.3, "categories": ["Electronics", "Home & Kitchen"]},
            {"name": "Holiday Special", "discount": 0.15, "categories": ["Beauty", "Fashion"]},
            {"name": "Clearance", "discount": 0.5, "categories": ["All"]}
        ]
        
        logger.info("TransactionGenerator initialized")
    
    def _load_master_data(self, dataset_name: str) -> List[Dict]:
        """Load master data from JSON file"""
        try:
            file_path = self.master_data_dir / f"{dataset_name}.json"
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Master data file not found: {file_path}")
            return []
    
    def _get_season(self, date: datetime) -> str:
        """Get season for date"""
        month = date.month
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        else:
            return "Fall"
    
    def _calculate_transaction_probability(self, timestamp: datetime) -> float:
        """Calculate transaction probability based on time patterns"""
        hour = timestamp.hour
        day = timestamp.weekday()
        month = timestamp.month
        
        hourly_prob = self.time_patterns["hourly"].get(hour, 0.1)
        daily_prob = self.time_patterns["daily"].get(day, 0.15)
        monthly_prob = self.time_patterns["monthly"].get(month, 0.1)
        
        # Adjust for weekends
        if day >= 5:  # Saturday or Sunday
            hourly_prob *= 1.2
        
        # Adjust for holidays (simplified)
        holiday_months = {12: 1.5, 11: 1.3, 7: 1.1}
        holiday_boost = holiday_months.get(month, 1.0)
        
        return hourly_prob * daily_prob * monthly_prob * holiday_boost
    
    def _select_customer(self, timestamp: datetime) -> Optional[Dict]:
        """Select a customer based on time and patterns"""
        if not self.customers:
            return None
        
        # Time-based customer selection
        hour = timestamp.hour
        if hour < 8 or hour > 22:
            # Late night/early morning - likely younger customers
            filtered = [c for c in self.customers 
                       if c.get("age", 30) < 40 and c.get("account_status") == "Active"]
        elif 9 <= hour <= 17:
            # Business hours - working professionals
            filtered = [c for c in self.customers 
                       if c.get("occupation") and c.get("account_status") == "Active"]
        else:
            # Evening - all active customers
            filtered = [c for c in self.customers if c.get("account_status") == "Active"]
        
        if not filtered:
            filtered = [c for c in self.customers if c.get("account_status") == "Active"]
        
        if not filtered:
            return random.choice(self.customers)
        
        # Weight by customer value
        weights = [c.get("lifetime_value", 1000) for c in filtered]
        return random.choices(filtered, weights=weights)[0]
    
    def _select_products(self, count: int = None) -> List[Dict]:
        """Select products for transaction"""
        if not self.products:
            return []
        
        # Determine number of items in transaction
        if count is None:
            # Based on transaction size distribution
            weights = [0.4, 0.3, 0.15, 0.1, 0.05]  # 1, 2, 3, 4, 5+ items
            count = random.choices([1, 2, 3, 4, 5], weights=weights)[0]
        
        # Select products with some patterns
        selected = []
        for _ in range(count):
            # Sometimes bundle related products
            if selected and random.random() < 0.3:
                category = selected[0]["category"]
                category_products = [p for p in self.products if p["category"] == category]
                if category_products:
                    product = random.choice(category_products)
                else:
                    product = random.choice(self.products)
            else:
                product = random.choice(self.products)
            
            # Determine quantity (usually 1, sometimes more)
            quantity = 1
            if random.random() < 0.1:  # 10% chance of multiple quantities
                quantity = random.randint(2, 5)
            
            # Add to selection with quantity
            product_with_qty = product.copy()
            product_with_qty["quantity"] = quantity
            selected.append(product_with_qty)
        
        return selected
    
    def _calculate_discount(self, products: List[Dict], customer: Dict, timestamp: datetime) -> Dict:
        """Calculate discounts for transaction"""
        total_amount = sum(p["price"] * p.get("quantity", 1) for p in products)
        
        discounts = []
        discount_amount = 0
        discount_reasons = []
        
        # Customer loyalty discount
        if customer.get("customer_tier") == "Platinum":
            discount = total_amount * 0.1
            discounts.append({"type": "loyalty", "amount": discount, "percentage": 10})
            discount_amount += discount
            discount_reasons.append("Platinum member discount")
        
        # Promotional discount
        season = self._get_season(timestamp)
        for promo in self.promotions:
            if random.random() < 0.2:  # 20% chance of promotion
                applicable = False
                if promo["categories"] == ["All"]:
                    applicable = True
                else:
                    product_categories = {p["category"] for p in products}
                    if any(cat in promo["categories"] for cat in product_categories):
                        applicable = True
                
                if applicable:
                    discount = total_amount * promo["discount"]
                    discounts.append({
                        "type": "promotion",
                        "name": promo["name"],
                        "amount": discount,
                        "percentage": promo["discount"] * 100
                    })
                    discount_amount += discount
                    discount_reasons.append(promo["name"])
                    break
        
        # Bulk discount for large orders
        total_items = sum(p.get("quantity", 1) for p in products)
        if total_items > 5:
            discount = total_amount * 0.05
            discounts.append({"type": "bulk", "amount": discount, "percentage": 5})
            discount_amount += discount
            discount_reasons.append("Bulk purchase discount")
        
        # First purchase discount
        if customer.get("total_orders", 0) == 0:
            discount = total_amount * 0.15
            discounts.append({"type": "first_purchase", "amount": discount, "percentage": 15})
            discount_amount += discount
            discount_reasons.append("First purchase discount")
        
        return {
            "discounts": discounts,
            "total_discount": round(discount_amount, 2),
            "discount_reasons": discount_reasons
        }
    
    def _process_payment(self, amount: float, customer: Dict) -> Dict:
        """Simulate payment processing"""
        payment_method = customer.get("preferred_payment_method", "Credit Card")
        if payment_method not in self.payment_methods:
            payment_method = random.choice(list(self.payment_methods.keys()))
        
        payment_info = self.payment_methods[payment_method]
        
        # Determine if payment succeeds
        success = random.random() < payment_info["success_rate"]
        
        # Add fraud detection
        is_fraud = False
        fraud_score = 0
        fraud_reasons = []
        
        for pattern in self.fraud_patterns:
            if random.random() < pattern["probability"]:
                is_fraud = True
                fraud_score += random.randint(20, 50)
                fraud_reasons.append(pattern["type"])
        
        # High value transaction fraud check
        if amount > 5000 and random.random() < 0.05:
            is_fraud = True
            fraud_score += 30
            fraud_reasons.append("high_value")
        
        # Generate payment details
        payment_id = f"PAY{random.randint(1000000, 9999999)}"
        
        if payment_method in ["Credit Card", "Debit Card"]:
            card_number = f"**** **** **** {random.randint(1000, 9999)}"
            payment_details = {
                "card_type": random.choice(["Visa", "Mastercard", "American Express"]),
                "last_four": str(random.randint(1000, 9999)),
                "expiry": f"{random.randint(1, 12):02d}/{random.randint(23, 30)}"
            }
        elif payment_method == "PayPal":
            payment_details = {"email": customer.get("email", "")}
        else:
            payment_details = {}
        
        return {
            "payment_id": payment_id,
            "method": payment_method,
            "amount": amount,
            "status": "approved" if success and not is_fraud else "declined",
            "success": success and not is_fraud,
            "processing_time_ms": payment_info["processing_time"] * 100 + random.randint(-20, 20),
            "fraud_detected": is_fraud,
            "fraud_score": fraud_score,
            "fraud_reasons": fraud_reasons if is_fraud else [],
            "payment_details": payment_details,
            "authorization_code": f"AUTH{random.randint(100000, 999999)}" if success else None,
            "decline_reason": random.choice(["insufficient_funds", "suspected_fraud", "card_expired"]) if not success else None
        }
    
    def _select_store(self, customer: Dict) -> Optional[Dict]:
        """Select store for transaction"""
        if not self.stores:
            return None
        
        # Prefer stores in customer's region
        customer_region = customer.get("address", {}).get("region", "North America")
        regional_stores = [s for s in self.stores 
                          if s.get("address", {}).get("region") == customer_region]
        
        if regional_stores:
            return random.choice(regional_stores)
        
        return random.choice(self.stores)
    
    def _generate_transaction_timestamp(self, base_time: datetime = None) -> datetime:
        """Generate realistic transaction timestamp"""
        if base_time is None:
            base_time = datetime.now() - timedelta(days=30)
        
        # Add random offset (up to 30 days in past, 1 day in future for pre-orders)
        days_offset = random.randint(-30, 1)
        hours_offset = random.randint(0, 23)
        minutes_offset = random.randint(0, 59)
        seconds_offset = random.randint(0, 59)
        
        timestamp = base_time + timedelta(
            days=days_offset,
            hours=hours_offset,
            minutes=minutes_offset,
            seconds=seconds_offset
        )
        
        # Adjust probability based on time patterns
        prob = self._calculate_transaction_probability(timestamp)
        if random.random() > prob:
            # Adjust to higher probability time
            hour_weights = list(self.time_patterns["hourly"].values())
            new_hour = random.choices(range(24), weights=hour_weights)[0]
            timestamp = timestamp.replace(hour=new_hour)
        
        return timestamp
    
    def generate_transaction(self, timestamp: datetime = None) -> Dict:
        """Generate a single transaction"""
        if timestamp is None:
            timestamp = self._generate_transaction_timestamp()
        
        # Select customer
        customer = self._select_customer(timestamp)
        
        # Select products
        products = self._select_products()
        
        # Calculate amounts
        subtotal = sum(p["price"] * p.get("quantity", 1) for p in products)
        
        # Apply discounts
        discount_info = self._calculate_discount(products, customer, timestamp)
        discounted_amount = subtotal - discount_info["total_discount"]
        
        # Calculate tax (simplified)
        tax_rate = random.uniform(0.05, 0.12)  # 5-12% tax
        tax_amount = round(discounted_amount * tax_rate, 2)
        
        # Shipping cost
        shipping_cost = 0
        shipping_method = "Standard"
        if discounted_amount < 50:  # Free shipping threshold
            shipping_cost = random.choice([0, 4.99, 9.99])
            if shipping_cost > 0:
                shipping_method = random.choice(["Standard", "Express"])
        
        # Final total
        total_amount = round(discounted_amount + tax_amount + shipping_cost, 2)
        
        # Process payment
        payment = self._process_payment(total_amount, customer)
        
        # Select store (online or in-store)
        is_online = random.random() < 0.7  # 70% online transactions
        store = None if is_online else self._select_store(customer)
        
        # Sales rep for in-store transactions
        sales_rep = None
        if store and random.random() < 0.6:  # 60% of in-store have sales rep
            sales_rep = random.choice(self.sales_reps) if self.sales_reps else None
        
        # Campaign attribution
        campaign = None
        if random.random() < 0.3:  # 30% attributed to campaigns
            active_campaigns = [c for c in self.campaigns if c.get("status") == "Active"]
            if active_campaigns:
                campaign = random.choice(active_campaigns)
        
        # Generate transaction ID
        transaction_id = f"TXN{self.transaction_id_counter}"
        self.transaction_id_counter += 1
        
        # Generate order ID (same for all items in transaction)
        order_id = f"ORD{self.order_id_counter}"
        self.order_id_counter += 1
        
        # Update customer metrics
        if customer:
            customer["total_orders"] = customer.get("total_orders", 0) + 1
            customer["total_spent"] = customer.get("total_spent", 0) + total_amount
            customer["last_purchase_date"] = timestamp.isoformat()
        
        # Build transaction
        transaction = {
            "transaction_id": transaction_id,
            "order_id": order_id,
            "timestamp": timestamp.isoformat(),
            "customer": {
                "customer_id": customer["customer_id"] if customer else "UNKNOWN",
                "email": customer["email"] if customer else "",
                "segment": customer.get("customer_segment", "Unknown") if customer else "Unknown"
            },
            "products": [
                {
                    "product_id": p["product_id"],
                    "product_name": p["product_name"],
                    "category": p["category"],
                    "brand": p["brand"],
                    "sku": p["sku"],
                    "quantity": p.get("quantity", 1),
                    "unit_price": p["price"],
                    "total_price": p["price"] * p.get("quantity", 1)
                } for p in products
            ],
            "financials": {
                "subtotal": round(subtotal, 2),
                "discounts": discount_info,
                "tax_amount": tax_amount,
                "tax_rate": round(tax_rate * 100, 2),
                "shipping_cost": shipping_cost,
                "shipping_method": shipping_method,
                "total_amount": total_amount,
                "currency": "USD"
            },
            "payment": payment,
            "channel": "Online" if is_online else "In-Store",
            "store": {
                "store_id": store["store_id"] if store else None,
                "store_name": store["store_name"] if store else None,
                "location": store["address"] if store else None
            } if store else None,
            "sales_rep": {
                "rep_id": sales_rep["rep_id"] if sales_rep else None,
                "name": f"{sales_rep['first_name']} {sales_rep['last_name']}" if sales_rep else None
            } if sales_rep else None,
            "campaign": {
                "campaign_id": campaign["campaign_id"] if campaign else None,
                "campaign_name": campaign["campaign_name"] if campaign else None,
                "utm_source": campaign.get("utm_parameters", {}).get("utm_source") if campaign else None
            } if campaign else None,
            "device": random.choice(["Desktop", "Mobile", "Tablet"]) if is_online else None,
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]) if is_online else None,
            "operating_system": random.choice(["Windows", "macOS", "iOS", "Android"]) if is_online else None,
            "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}" if is_online else None,
            "user_agent": "Mozilla/5.0..." if is_online else None,
            "session_id": str(uuid.uuid4()) if is_online else None,
            "fulfillment": {
                "status": random.choice(["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]),
                "carrier": random.choice(["UPS", "FedEx", "USPS", "DHL"]) if is_online else None,
                "tracking_number": f"TRK{random.randint(1000000000, 9999999999)}" if is_online and random.random() < 0.8 else None,
                "estimated_delivery": (timestamp + timedelta(days=random.randint(2, 7))).isoformat() if is_online else None
            },
            "metadata": {
                "season": self._get_season(timestamp),
                "is_weekend": timestamp.weekday() >= 5,
                "is_holiday": random.random() < 0.1,  # 10% chance of holiday
                "customer_lifetime_value": customer.get("lifetime_value", 0) if customer else 0,
                "transaction_complexity": len(products),
                "generated_at": datetime.now().isoformat()
            }
        }
        
        return transaction
    
    def generate_batch(self, count: int, output_dir: str = "data", 
                       output_format: str = "json", save: bool = True) -> List[Dict]:
        """Generate a batch of transactions"""
        logger.info(f"Generating {count} transactions...")
        
        transactions = []
        batch_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
        
        for i in range(count):
            # Progress logging
            if (i + 1) % 100 == 0:
                logger.info(f"Generated {i + 1}/{count} transactions")
            
            transaction = self.generate_transaction()
            transactions.append(transaction)
        
        logger.info(f"Generated {len(transactions)} transactions")
        
        if save:
            self._save_transactions(transactions, output_dir, output_format, batch_id)
        
        return transactions
    
    def _save_transactions(self, transactions: List[Dict], output_dir: str, 
                          output_format: str, batch_id: str):
        """Save transactions to file"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"transactions_{timestamp}_{batch_id}"
        
        if output_format == "json":
            filepath = output_path / f"{filename}.json"
            with open(filepath, 'w') as f:
                json.dump(transactions, f, indent=2, default=str)
            logger.info(f"Saved {len(transactions)} transactions to {filepath}")
        
        elif output_format == "csv":
            # Flatten transactions for CSV
            flattened = []
            for tx in transactions:
                flat_tx = self._flatten_transaction(tx)
                flattened.append(flat_tx)
            
            filepath = output_path / f"{filename}.csv"
            df = pd.DataFrame(flattened)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(transactions)} transactions to {filepath}")
        
        elif output_format == "parquet":
            filepath = output_path / f"{filename}.parquet"
            df = pd.DataFrame(transactions)
            df.to_parquet(filepath, index=False)
            logger.info(f"Saved {len(transactions)} transactions to {filepath}")
    
    def _flatten_transaction(self, transaction: Dict) -> Dict:
        """Flatten nested transaction for CSV output"""
        flat = {}
        
        # Basic fields
        flat["transaction_id"] = transaction["transaction_id"]
        flat["order_id"] = transaction["order_id"]
        flat["timestamp"] = transaction["timestamp"]
        
        # Customer fields
        flat["customer_id"] = transaction["customer"]["customer_id"]
        flat["customer_email"] = transaction["customer"]["email"]
        flat["customer_segment"] = transaction["customer"]["segment"]
        
        # Financial fields
        flat["subtotal"] = transaction["financials"]["subtotal"]
        flat["total_discount"] = transaction["financials"]["discounts"]["total_discount"]
        flat["tax_amount"] = transaction["financials"]["tax_amount"]
        flat["tax_rate"] = transaction["financials"]["tax_rate"]
        flat["shipping_cost"] = transaction["financials"]["shipping_cost"]
        flat["total_amount"] = transaction["financials"]["total_amount"]
        flat["currency"] = transaction["financials"]["currency"]
        
        # Payment fields
        flat["payment_method"] = transaction["payment"]["method"]
        flat["payment_status"] = transaction["payment"]["status"]
        flat["payment_success"] = transaction["payment"]["success"]
        flat["fraud_detected"] = transaction["payment"]["fraud_detected"]
        flat["fraud_score"] = transaction["payment"]["fraud_score"]
        
        # Channel and store
        flat["channel"] = transaction["channel"]
        flat["store_id"] = transaction.get("store", {}).get("store_id")
        flat["store_name"] = transaction.get("store", {}).get("store_name")
        
        # Campaign
        flat["campaign_id"] = transaction.get("campaign", {}).get("campaign_id")
        flat["campaign_name"] = transaction.get("campaign", {}).get("campaign_name")
        
        # Fulfillment
        flat["fulfillment_status"] = transaction["fulfillment"]["status"]
        flat["carrier"] = transaction["fulfillment"]["carrier"]
        
        # Metadata
        flat["season"] = transaction["metadata"]["season"]
        flat["is_weekend"] = transaction["metadata"]["is_weekend"]
        flat["transaction_complexity"] = transaction["metadata"]["transaction_complexity"]
        
        # Products (as JSON string for CSV)
        flat["products"] = json.dumps(transaction["products"])
        flat["discount_details"] = json.dumps(transaction["financials"]["discounts"])
        
        return flat
    
    def generate_real_time_stream(self, duration_minutes: int = 5, 
                                 transactions_per_minute: int = 100,
                                 stream_to_kinesis: bool = False,
                                 kinesis_stream_name: str = None):
        """Generate real-time stream of transactions"""
        logger.info(f"Starting real-time stream for {duration_minutes} minutes "
                   f"at {transactions_per_minute} tpm")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        transactions_generated = 0
        
        if stream_to_kinesis and kinesis_stream_name:
            kinesis_client = boto3.client('kinesis')
        
        while datetime.now() < end_time:
            minute_start = datetime.now()
            
            # Generate transactions for this minute
            target_count = int(transactions_per_minute * random.uniform(0.8, 1.2))  # +/-20% variation
            logger.info(f"Generating {target_count} transactions for this minute")
            
            for _ in range(target_count):
                # Generate transaction with current timestamp
                transaction = self.generate_transaction(datetime.now())
                transactions_generated += 1
                
                # Stream to Kinesis if enabled
                if stream_to_kinesis and kinesis_stream_name:
                    try:
                        response = kinesis_client.put_record(
                            StreamName=kinesis_stream_name,
                            Data=json.dumps(transaction),
                            PartitionKey=transaction["customer"]["customer_id"]
                        )
                        logger.debug(f"Sent transaction to Kinesis: {response['SequenceNumber']}")
                    except Exception as e:
                        logger.error(f"Failed to send to Kinesis: {str(e)}")
                
                # Small delay between transactions
                time.sleep(random.uniform(0.001, 0.1))
            
            # Wait for next minute
            elapsed = (datetime.now() - minute_start).total_seconds()
            if elapsed < 60:
                time.sleep(60 - elapsed)
        
        logger.info(f"Real-time stream completed. Generated {transactions_generated} transactions")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sales transactions")
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch",
                       help="Generation mode: batch or stream")
    parser.add_argument("--count", type=int, default=1000,
                       help="Number of transactions to generate (batch mode)")
    parser.add_argument("--duration", type=int, default=5,
                       help="Duration in minutes (stream mode)")
    parser.add_argument("--tpm", type=int, default=100,
                       help="Transactions per minute (stream mode)")
    parser.add_argument("--output-dir", default="data",
                       help="Output directory for generated data")
    parser.add_argument("--format", choices=["json", "csv", "parquet"], default="json",
                       help="Output format")
    parser.add_argument("--master-data-dir", default="data",
                       help="Directory containing master data")
    parser.add_argument("--seed", type=int, default=42,
                       help="Random seed for reproducibility")
    parser.add_argument("--stream-to-kinesis", action="store_true",
                       help="Stream transactions to Kinesis (stream mode)")
    parser.add_argument("--kinesis-stream",
                       help="Kinesis stream name")
    
    args = parser.parse_args()
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Initialize generator
    generator = TransactionGenerator(
        master_data_dir=args.master_data_dir,
        seed=args.seed
    )
    
    try:
        if args.mode == "batch":
            transactions = generator.generate_batch(
                count=args.count,
                output_dir=args.output_dir,
                output_format=args.format,
                save=True
            )
            logger.info(f"Batch generation completed: {len(transactions)} transactions")
            
        elif args.mode == "stream":
            generator.generate_real_time_stream(
                duration_minutes=args.duration,
                transactions_per_minute=args.tpm,
                stream_to_kinesis=args.stream_to_kinesis,
                kinesis_stream_name=args.kinesis_stream
            )
            logger.info("Real-time stream completed")
    
    except Exception as e:
        logger.error(f"Transaction generation failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()