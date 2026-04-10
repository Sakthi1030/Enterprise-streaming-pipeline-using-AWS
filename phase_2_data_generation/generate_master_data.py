#!/usr/bin/env python3
"""
Generate master/reference data for the Sales & Marketing Platform.
This includes:
- Products catalog
- Customers database
- Sales representatives
- Campaigns
- Store locations
"""

import json
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
from pathlib import Path
import boto3
from faker import Faker
import pandas as pd
import yaml

# Configure logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/master_data_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MasterDataGenerator:
    """Generate master/reference data for the platform"""
    
    def __init__(self, output_dir: str = "data", fake_seed: int = 42):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.fake = Faker()
        Faker.seed(fake_seed)
        random.seed(fake_seed)
        
        # Product categories and subcategories
        self.product_categories = {
            "Electronics": ["Smartphones", "Laptops", "Tablets", "Wearables", "Accessories"],
            "Fashion": ["Clothing", "Footwear", "Accessories", "Watches", "Jewelry"],
            "Home & Kitchen": ["Furniture", "Appliances", "Cookware", "Home Decor", "Lighting"],
            "Beauty": ["Skincare", "Makeup", "Fragrances", "Haircare", "Personal Care"],
            "Sports": ["Fitness", "Outdoor", "Team Sports", "Athletic Apparel", "Equipment"]
        }
        
        # Regions and cities
        self.regions = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
        self.us_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                         "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
        self.eu_cities = ["London", "Berlin", "Paris", "Madrid", "Rome", 
                         "Amsterdam", "Brussels", "Vienna", "Prague", "Warsaw"]
        
        # Product brands
        self.brands = {
            "Electronics": ["Apple", "Samsung", "Sony", "Microsoft", "Dell", "HP", "Lenovo"],
            "Fashion": ["Nike", "Adidas", "Zara", "H&M", "Gucci", "Louis Vuitton", "Prada"],
            "Home & Kitchen": ["IKEA", "Williams-Sonoma", "KitchenAid", "Ninja", "Dyson"],
            "Beauty": ["L'Oreal", "Estee Lauder", "MAC", "NARS", "Clinique", "Kiehl's"],
            "Sports": ["Nike", "Adidas", "Under Armour", "Puma", "Reebok", "Columbia"]
        }
        
        logger.info("MasterDataGenerator initialized")
    
    def generate_products(self, count: int = 1000) -> List[Dict]:
        """Generate product catalog"""
        logger.info(f"Generating {count} products...")
        
        products = []
        product_id = 1000
        
        for _ in range(count):
            category = random.choice(list(self.product_categories.keys()))
            subcategory = random.choice(self.product_categories[category])
            brand = random.choice(self.brands[category])
            
            # Generate product name
            product_names = {
                "Smartphones": [f"{brand} Phone", f"{brand} Smartphone", f"{brand} Mobile"],
                "Laptops": [f"{brand} Laptop", f"{brand} Notebook", f"{brand} Ultrabook"],
                "Clothing": [f"{brand} T-Shirt", f"{brand} Jacket", f"{brand} Dress"],
                "Footwear": [f"{brand} Sneakers", f"{brand} Running Shoes", f"{brand} Boots"],
                "Furniture": [f"{brand} Chair", f"{brand} Table", f"{brand} Sofa"],
                "Skincare": [f"{brand} Moisturizer", f"{brand} Serum", f"{brand} Cleanser"],
                "Fitness": [f"{brand} Dumbbells", f"{brand} Yoga Mat", f"{brand} Resistance Bands"]
            }
            
            name_template = random.choice(product_names.get(subcategory, [f"{brand} {subcategory}"]))
            name = f"{name_template} {random.choice(['Pro', 'Plus', 'Max', 'Elite', 'Premium'])}"
            
            # Price based on category and brand
            base_price = {
                "Electronics": random.randint(300, 2000),
                "Fashion": random.randint(50, 500),
                "Home & Kitchen": random.randint(100, 1000),
                "Beauty": random.randint(20, 300),
                "Sports": random.randint(30, 400)
            }[category]
            
            # Add brand premium
            brand_premium = {"Apple": 1.5, "Gucci": 2.0, "Louis Vuitton": 2.5, "Dyson": 1.8}
            price_multiplier = brand_premium.get(brand, 1.0)
            price = round(base_price * price_multiplier, 2)
            
            # Cost price (60-80% of selling price)
            cost_price = round(price * random.uniform(0.6, 0.8), 2)
            
            # Generate attributes
            material = random.choice(["Plastic", "Metal", "Fabric", "Leather", "Wood"])
            color = random.choice(["Black", "White", "Silver", "Blue", "Red", "Gold"])
            
            product = {
                "product_id": f"PROD{product_id}",
                "product_name": name,
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "sku": f"SKU-{random.randint(10000, 99999)}-{random.randint(100, 999)}",
                "price": price,
                "cost_price": cost_price,
                "profit_margin": round(((price - cost_price) / price) * 100, 2),
                "stock_quantity": random.randint(0, 500),
                "reorder_level": random.randint(10, 50),
                "supplier_id": f"SUPP{random.randint(100, 999)}",
                "manufacturer": brand,
                "weight_kg": round(random.uniform(0.1, 20.0), 2),
                "dimensions": f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(10, 100)}cm",
                "color": color,
                "material": material,
                "warranty_months": random.choice([12, 24, 36, 60]),
                "rating": round(random.uniform(3.0, 5.0), 1),
                "reviews_count": random.randint(0, 10000),
                "is_active": random.choice([True, True, True, False]),  # 75% active
                "date_added": self.fake.date_between(start_date='-2y', end_date='today').isoformat(),
                "last_updated": datetime.now().isoformat(),
                "tags": [category.lower(), subcategory.lower(), brand.lower()],
                "description": f"High-quality {subcategory.lower()} from {brand}",
                "features": [
                    f"Premium {material} construction",
                    f"{random.choice(['Advanced', 'Innovative', 'Cutting-edge'])} technology",
                    f"{random.choice(['Energy efficient', 'Eco-friendly', 'Sustainable'])} design"
                ],
                "metadata": {
                    "seasonal": random.choice([True, False]),
                    "limited_edition": random.choice([True, False]),
                    "bestseller": random.choice([True, False]),
                    "new_arrival": random.random() > 0.8
                }
            }
            
            products.append(product)
            product_id += 1
            
        logger.info(f"Generated {len(products)} products")
        return products
    
    def generate_customers(self, count: int = 5000) -> List[Dict]:
        """Generate customer database"""
        logger.info(f"Generating {count} customers...")
        
        customers = []
        customer_id = 10000
        
        for _ in range(count):
            # Generate demographics
            age = random.randint(18, 80)
            gender = random.choice(["Male", "Female", "Other"])
            income_bracket = random.choice(["Low", "Middle", "High", "Very High"])
            
            # Generate location
            region = random.choice(self.regions)
            if region == "North America":
                city = random.choice(self.us_cities)
                country = "USA"
                state = random.choice(["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"])
            elif region == "Europe":
                city = random.choice(self.eu_cities)
                country = random.choice(["UK", "Germany", "France", "Spain", "Italy"])
                state = ""
            else:
                city = self.fake.city()
                country = self.fake.country()
                state = self.fake.state() if random.choice([True, False]) else ""
            
            # Customer segmentation
            segments = ["New", "Returning", "Loyal", "VIP", "At Risk", "Churned"]
            segment_weights = [0.3, 0.25, 0.2, 0.1, 0.1, 0.05]
            segment = random.choices(segments, weights=segment_weights)[0]
            
            # Calculate customer value metrics
            if segment == "VIP":
                lifetime_value = random.randint(5000, 50000)
                avg_order_value = random.randint(500, 2000)
                purchase_frequency = random.randint(12, 48)
            elif segment == "Loyal":
                lifetime_value = random.randint(1000, 5000)
                avg_order_value = random.randint(200, 500)
                purchase_frequency = random.randint(6, 12)
            else:
                lifetime_value = random.randint(0, 1000)
                avg_order_value = random.randint(50, 200)
                purchase_frequency = random.randint(1, 6)
            
            customer = {
                "customer_id": f"CUST{customer_id}",
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "date_of_birth": self.fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
                "age": age,
                "gender": gender,
                "income_bracket": income_bracket,
                "occupation": self.fake.job(),
                "company": self.fake.company() if random.choice([True, False]) else "",
                "address": {
                    "street": self.fake.street_address(),
                    "city": city,
                    "state": state,
                    "country": country,
                    "postal_code": self.fake.postcode(),
                    "region": region
                },
                "account_created": self.fake.date_between(start_date='-5y', end_date='today').isoformat(),
                "last_login": self.fake.date_between(start_date='-30d', end_date='today').isoformat() if random.random() > 0.3 else "",
                "account_status": random.choice(["Active", "Inactive", "Suspended", "Closed"]),
                "customer_segment": segment,
                "lifetime_value": lifetime_value,
                "avg_order_value": avg_order_value,
                "purchase_frequency": purchase_frequency,  # times per year
                "last_purchase_date": self.fake.date_between(start_date='-90d', end_date='today').isoformat() if random.random() > 0.4 else "",
                "total_orders": random.randint(0, purchase_frequency * 2),
                "total_spent": lifetime_value,
                "preferred_channel": random.choice(["Email", "Mobile App", "Website", "In-Store"]),
                "preferred_category": random.choice(list(self.product_categories.keys())),
                "loyalty_points": random.randint(0, 50000),
                "newsletter_subscribed": random.choice([True, False]),
                "marketing_opt_in": random.choice([True, False]),
                "customer_tier": random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
                "credit_score": random.randint(300, 850) if random.choice([True, False]) else None,
                "preferred_payment_method": random.choice(["Credit Card", "PayPal", "Apple Pay", "Bank Transfer"]),
                "shipping_preference": random.choice(["Standard", "Express", "Next Day"]),
                "social_media": {
                    "twitter": f"@{self.fake.user_name()}" if random.choice([True, False]) else "",
                    "instagram": f"@{self.fake.user_name()}" if random.choice([True, False]) else "",
                    "facebook": f"facebook.com/{self.fake.user_name()}" if random.choice([True, False]) else ""
                },
                "notes": random.choice(["", "Prefers eco-friendly products", "Frequent returner", "VIP customer"])
            }
            
            customers.append(customer)
            customer_id += 1
            
        logger.info(f"Generated {len(customers)} customers")
        return customers
    
    def generate_sales_reps(self, count: int = 50) -> List[Dict]:
        """Generate sales representatives"""
        logger.info(f"Generating {count} sales representatives...")
        
        sales_reps = []
        rep_id = 100
        
        regions_territories = {
            "North America": ["East Coast", "West Coast", "Midwest", "South", "Canada"],
            "Europe": ["Northern Europe", "Southern Europe", "Eastern Europe", "Western Europe"],
            "Asia Pacific": ["Southeast Asia", "East Asia", "Australia/NZ", "India"],
            "Latin America": ["Brazil", "Mexico", "Andean", "Southern Cone"],
            "Middle East": ["GCC", "Levant", "North Africa"]
        }
        
        for _ in range(count):
            region = random.choice(self.regions)
            territory = random.choice(regions_territories[region])
            
            # Performance metrics
            quota = random.randint(100000, 500000)
            ytd_sales = random.randint(int(quota * 0.3), int(quota * 1.5))
            attainment = round((ytd_sales / quota) * 100, 2)
            
            rep = {
                "rep_id": f"REP{rep_id}",
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "region": region,
                "territory": territory,
                "manager_id": f"REP{random.choice([1, 2, 3, 4, 5])}" if rep_id > 105 else "",
                "hire_date": self.fake.date_between(start_date='-10y', end_date='-1y').isoformat(),
                "employment_type": random.choice(["Full-time", "Part-time", "Contract"]),
                "base_salary": random.randint(50000, 120000),
                "commission_rate": round(random.uniform(0.05, 0.15), 3),
                "quota": quota,
                "ytd_sales": ytd_sales,
                "attainment_percentage": attainment,
                "total_customers": random.randint(10, 200),
                "avg_deal_size": random.randint(1000, 50000),
                "close_rate": round(random.uniform(0.1, 0.4), 3),
                "active_deals": random.randint(1, 20),
                "pipeline_value": random.randint(50000, 500000),
                "skills": random.sample(["Negotiation", "CRM", "Presentation", "Closing", "Prospecting"], 3),
                "certifications": random.sample(["Salesforce", "Google Analytics", "HubSpot", "Microsoft Dynamics"], random.randint(0, 3)),
                "performance_rating": round(random.uniform(3.0, 5.0), 1),
                "last_training": self.fake.date_between(start_date='-180d', end_date='today').isoformat() if random.choice([True, False]) else "",
                "status": random.choice(["Active", "On Leave", "Terminated"]),
                "notes": random.choice(["", "Top performer", "Needs coaching", "Expert in enterprise sales"])
            }
            
            sales_reps.append(rep)
            rep_id += 1
            
        logger.info(f"Generated {len(sales_reps)} sales representatives")
        return sales_reps
    
    def generate_campaigns(self, count: int = 20) -> List[Dict]:
        """Generate marketing campaigns"""
        logger.info(f"Generating {count} campaigns...")
        
        campaigns = []
        campaign_id = 1000
        
        campaign_types = ["Email", "Social Media", "Search Ads", "Display Ads", "TV", "Radio", "Print", "Influencer"]
        campaign_statuses = ["Planning", "Active", "Paused", "Completed", "Cancelled"]
        campaign_channels = ["Digital", "Traditional", "Hybrid"]
        
        for _ in range(count):
            start_date = self.fake.date_between(start_date='-90d', end_date='+30d')
            end_date = start_date + timedelta(days=random.randint(7, 90))
            status = random.choice(campaign_statuses)
            
            # Budget allocation
            total_budget = random.randint(10000, 500000)
            spent_budget = random.randint(0, total_budget) if status != "Planning" else 0
            
            # Campaign performance metrics
            if status in ["Active", "Completed"]:
                impressions = random.randint(10000, 1000000)
                clicks = random.randint(1000, 100000)
                conversions = random.randint(100, 10000)
                revenue = random.randint(10000, 500000)
                ctr = round((clicks / impressions) * 100, 4) if impressions > 0 else 0
                conversion_rate = round((conversions / clicks) * 100, 2) if clicks > 0 else 0
                roas = round(revenue / spent_budget, 2) if spent_budget > 0 else 0
            else:
                impressions = clicks = conversions = revenue = ctr = conversion_rate = roas = 0
            
            campaign = {
                "campaign_id": f"CAMP{campaign_id}",
                "campaign_name": f"{random.choice(['Summer', 'Winter', 'Spring', 'Fall', 'Holiday', 'Back to School'])} {random.choice(['Sale', 'Promotion', 'Campaign', 'Event'])} {random.randint(2023, 2024)}",
                "campaign_type": random.choice(campaign_types),
                "channel": random.choice(campaign_channels),
                "status": status,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "target_audience": random.choice(["All Customers", "New Customers", "Loyal Customers", "High Value", "Cart Abandoners"]),
                "target_segment": random.choice(list(self.product_categories.keys())),
                "objective": random.choice(["Brand Awareness", "Lead Generation", "Sales", "Customer Retention"]),
                "budget": total_budget,
                "spent": spent_budget,
                "remaining_budget": total_budget - spent_budget,
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": revenue,
                "ctr": ctr,
                "conversion_rate": conversion_rate,
                "cpc": round(spent_budget / clicks, 2) if clicks > 0 else 0,
                "cpa": round(spent_budget / conversions, 2) if conversions > 0 else 0,
                "roas": roas,
                "kpis": {
                    "target_ctr": round(random.uniform(1.0, 5.0), 2),
                    "target_conversion_rate": round(random.uniform(2.0, 10.0), 2),
                    "target_roas": round(random.uniform(3.0, 10.0), 2),
                    "target_cpa": random.randint(50, 500)
                },
                "creative_assets": random.randint(1, 10),
                "landing_page": f"https://example.com/{random.choice(['offer', 'sale', 'promo'])}/{campaign_id}",
                "utm_parameters": {
                    "utm_source": random.choice(["email", "facebook", "google", "instagram"]),
                    "utm_medium": random.choice(["cpc", "email", "social"]),
                    "utm_campaign": f"campaign_{campaign_id}",
                    "utm_content": random.choice(["banner", "text", "video"]),
                    "utm_term": random.choice(["sale", "discount", "offer"])
                },
                "team_lead": f"REP{random.randint(100, 120)}",
                "agency": random.choice(["In-house", "Agency A", "Agency B", "Agency C"]) if random.choice([True, False]) else "",
                "notes": random.choice(["", "High performing campaign", "Needs optimization", "Seasonal campaign"])
            }
            
            campaigns.append(campaign)
            campaign_id += 1
            
        logger.info(f"Generated {len(campaigns)} campaigns")
        return campaigns
    
    def generate_store_locations(self, count: int = 20) -> List[Dict]:
        """Generate store locations"""
        logger.info(f"Generating {count} store locations...")
        
        stores = []
        store_id = 100
        
        store_types = ["Flagship", "Regular", "Outlet", "Pop-up", "Warehouse"]
        store_formats = ["Mall", "Street", "Airport", "Shopping Center", "Standalone"]
        
        for _ in range(count):
            region = random.choice(self.regions)
            if region == "North America":
                city = random.choice(self.us_cities)
                country = "USA"
            elif region == "Europe":
                city = random.choice(self.eu_cities)
                country = random.choice(["UK", "Germany", "France", "Spain", "Italy"])
            else:
                city = self.fake.city()
                country = self.fake.country()
            
            # Store size and metrics
            size_sqft = random.randint(1000, 10000)
            opening_date = self.fake.date_between(start_date='-10y', end_date='-1y')
            
            store = {
                "store_id": f"STORE{store_id}",
                "store_name": f"{city} {random.choice(['Store', 'Outlet', 'Boutique', 'Superstore'])}",
                "store_type": random.choice(store_types),
                "format": random.choice(store_formats),
                "address": {
                    "street": self.fake.street_address(),
                    "city": city,
                    "state": self.fake.state() if country == "USA" else "",
                    "country": country,
                    "postal_code": self.fake.postcode(),
                    "region": region
                },
                "coordinates": {
                    "latitude": float(round(self.fake.latitude(), 6)),
                    "longitude": float(round(self.fake.longitude(), 6))
                },
                "phone": self.fake.phone_number(),
                "email": f"store{store_id}@company.com",
                "manager_id": f"REP{random.randint(100, 150)}",
                "opening_date": opening_date.isoformat(),
                "size_sqft": size_sqft,
                "employee_count": random.randint(5, 50),
                "opening_hours": {
                    "monday": "9:00-21:00",
                    "tuesday": "9:00-21:00",
                    "wednesday": "9:00-21:00",
                    "thursday": "9:00-21:00",
                    "friday": "9:00-22:00",
                    "saturday": "10:00-22:00",
                    "sunday": "11:00-19:00"
                },
                "services": random.sample(["In-store pickup", "Returns", "Repairs", "Installation", "Gift wrapping", "Personal shopping"], 3),
                "avg_daily_customers": random.randint(100, 2000),
                "avg_transaction_value": random.randint(50, 500),
                "monthly_sales": random.randint(50000, 500000),
                "inventory_value": random.randint(100000, 1000000),
                "parking_spaces": random.randint(0, 100),
                "has_cafe": random.choice([True, False]),
                "has_wifi": random.choice([True, False]),
                "is_24_hours": random.choice([True, False]),
                "last_renovation": self.fake.date_between(start_date='-5y', end_date='today').isoformat() if random.choice([True, False]) else "",
                "status": random.choice(["Active", "Temporarily Closed", "Under Renovation", "Permanently Closed"]),
                "notes": random.choice(["", "High performing store", "Located in prime location", "Low foot traffic"])
            }
            
            stores.append(store)
            store_id += 1
            
        logger.info(f"Generated {len(stores)} store locations")
        return stores
    
    def save_to_json(self, data: List[Dict], filename: str):
        """Save data to JSON file"""
        output_path = self.output_dir / f"{filename}.json"
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(data)} records to {output_path}")
    
    def save_to_csv(self, data: List[Dict], filename: str):
        """Save data to CSV file"""
        output_path = self.output_dir / f"{filename}.csv"
        
        # Flatten nested dictionaries for CSV
        flattened_data = []
        for item in data:
            flat_item = {}
            for key, value in item.items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        flat_item[f"{key}_{subkey}"] = subvalue
                elif isinstance(value, list):
                    flat_item[key] = json.dumps(value)
                else:
                    flat_item[key] = value
            flattened_data.append(flat_item)
        
        df = pd.DataFrame(flattened_data)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(data)} records to {output_path}")
    
    def save_to_parquet(self, data: List[Dict], filename: str):
        """Save data to Parquet file"""
        output_path = self.output_dir / f"{filename}.parquet"
        df = pd.DataFrame(data)
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(data)} records to {output_path}")
    
    def upload_to_s3(self, data: List[Dict], filename: str, bucket_name: str, s3_prefix: str = ""):
        """Upload data to S3 bucket"""
        try:
            s3_client = boto3.client('s3')
            
            # Save to local temp file
            temp_path = Path(f"/tmp/{filename}.json")
            with open(temp_path, 'w') as f:
                json.dump(data, f)
            
            # Upload to S3
            s3_key = f"{s3_prefix}/{filename}.json" if s3_prefix else f"{filename}.json"
            s3_client.upload_file(str(temp_path), bucket_name, s3_key)
            
            logger.info(f"Uploaded {filename}.json to s3://{bucket_name}/{s3_key}")
            
            # Clean up temp file
            temp_path.unlink()
            
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
    
    def generate_all_data(self, upload_to_s3: bool = False, s3_bucket: str = None):
        """Generate all master data"""
        logger.info("Starting generation of all master data...")
        
        # Generate all datasets
        products = self.generate_products(1000)
        customers = self.generate_customers(5000)
        sales_reps = self.generate_sales_reps(50)
        campaigns = self.generate_campaigns(20)
        stores = self.generate_store_locations(20)
        
        # Save to files
        self.save_to_json(products, "products")
        self.save_to_csv(products, "products")
        self.save_to_parquet(products, "products")
        
        self.save_to_json(customers, "customers")
        self.save_to_csv(customers, "customers")
        self.save_to_parquet(customers, "customers")
        
        self.save_to_json(sales_reps, "sales_reps")
        self.save_to_csv(sales_reps, "sales_reps")
        
        self.save_to_json(campaigns, "campaigns")
        self.save_to_csv(campaigns, "campaigns")
        
        self.save_to_json(stores, "stores")
        self.save_to_csv(stores, "stores")
        
        # Generate metadata file
        metadata = {
            "generated_at": datetime.now().isoformat(),
            "datasets": {
                "products": len(products),
                "customers": len(customers),
                "sales_reps": len(sales_reps),
                "campaigns": len(campaigns),
                "stores": len(stores)
            },
            "output_formats": ["json", "csv", "parquet"],
            "schema_version": "1.0"
        }
        
        with open(self.output_dir / "metadata.yaml", 'w') as f:
            yaml.dump(metadata, f)
        
        # Upload to S3 if requested
        if upload_to_s3 and s3_bucket:
            self.upload_to_s3(products, "products", s3_bucket, "master_data")
            self.upload_to_s3(customers, "customers", s3_bucket, "master_data")
            self.upload_to_s3(sales_reps, "sales_reps", s3_bucket, "master_data")
            self.upload_to_s3(campaigns, "campaigns", s3_bucket, "master_data")
            self.upload_to_s3(stores, "stores", s3_bucket, "master_data")
        
        logger.info("Master data generation completed successfully!")
        
        return {
            "products": products,
            "customers": customers,
            "sales_reps": sales_reps,
            "campaigns": campaigns,
            "stores": stores
        }

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate master data for Sales & Marketing Platform")
    parser.add_argument("--output-dir", default="data", help="Output directory for generated data")
    parser.add_argument("--upload-to-s3", action="store_true", help="Upload generated data to S3")
    parser.add_argument("--s3-bucket", help="S3 bucket name for upload")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Generate data
    generator = MasterDataGenerator(output_dir=args.output_dir, fake_seed=args.seed)
    
    try:
        generator.generate_all_data(
            upload_to_s3=args.upload_to_s3,
            s3_bucket=args.s3_bucket
        )
        logger.info("Master data generation completed successfully!")
    except Exception as e:
        logger.error(f"Failed to generate master data: {str(e)}")
        raise

if __name__ == "__main__":
    main()