import pymongo
import pandas as pd
import re
from typing import Dict, Tuple, Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Predefined lists for Indian states and their variations
INDIAN_STATES = {
    'Andhra Pradesh': ['andhra', 'ap'],
    'Arunachal Pradesh': ['arunachal'],
    'Assam': ['assam'],
    'Bihar': ['bihar'],
    'Chhattisgarh': ['chhattisgarh', 'chattisgarh'],
    'Goa': ['goa'],
    'Gujarat': ['gujarat', 'gujrat'],
    'Haryana': ['haryana'],
    'Himachal Pradesh': ['himachal'],
    'Jharkhand': ['jharkhand'],
    'Karnataka': ['karnataka', 'karnatka'],
    'Kerala': ['kerala'],
    'Madhya Pradesh': ['madhya pradesh', 'mp'],
    'Maharashtra': ['maharashtra'],
    'Manipur': ['manipur'],
    'Meghalaya': ['meghalaya'],
    'Mizoram': ['mizoram'],
    'Nagaland': ['nagaland'],
    'Odisha': ['odisha', 'orissa'],
    'Punjab': ['punjab'],
    'Rajasthan': ['rajasthan'],
    'Sikkim': ['sikkim'],
    'Tamil Nadu': ['tamil nadu', 'tamilnadu'],
    'Telangana': ['telangana'],
    'Tripura': ['tripura'],
    'Uttar Pradesh': ['uttar pradesh', 'up'],
    'Uttarakhand': ['uttarakhand', 'uttaranchal'],
    'West Bengal': ['west bengal', 'wb'],
    'Delhi': ['delhi', 'new delhi'],
    'Jammu and Kashmir': ['jammu', 'kashmir', 'jammu and kashmir', 'j&k'],
    'Ladakh': ['ladakh'],
    'Puducherry': ['puducherry', 'pondicherry'],
    'Andaman and Nicobar Islands': ['andaman', 'nicobar'],
    'Chandigarh': ['chandigarh'],
    'Dadra and Nagar Haveli': ['dadra', 'nagar haveli'],
    'Daman and Diu': ['daman', 'diu'],
    'Lakshadweep': ['lakshadweep'],
}

# Major cities list to help with city identification
MAJOR_CITIES = [
    'mumbai', 'delhi', 'bangalore', 'bengaluru', 'hyderabad', 'ahmedabad', 
    'chennai', 'kolkata', 'surat', 'pune', 'jaipur', 'lucknow', 'kanpur', 
    'nagpur', 'indore', 'thane', 'bhopal', 'visakhapatnam', 'vizag', 'pimpri',
    'vadodara', 'ghaziabad', 'ludhiana', 'agra', 'nashik', 'faridabad', 
    'meerut', 'rajkot', 'varanasi', 'srinagar', 'aurangabad', 'dhanbad', 
    'amritsar', 'allahabad', 'gwalior', 'jabalpur', 'coimbatore', 'vijayawada',
    'jodhpur', 'madurai', 'raipur', 'kota', 'chandigarh', 'guwahati', 'mysore',
    'mysuru', 'thiruvananthapuram', 'trivandrum', 'hubli', 'dharwad',
    'salem', 'noida', 'kollam', 'vellore', 'pondicherry', 'warangal',
    'siliguri', 'mangalore', 'kozhikode', 'tiruchirappalli', 'bhubaneswar',
    'tirunelveli', 'ujjain', 'jamshedpur', 'patna', 'dehradun', 'ranchi',
    'jalgaon', 'jamnagar', 'ambala', 'hosur', 'meerut', 'anantapur', 'belgaum',
    'bilaspur', 'bhiwandi', 'gulbarga', 'gorakhpur', 'jhansi', 'aligarh',
    'kurnool', 'bareilly', 'moradabad', 'jalna', 'kakinada', 'tirupati',
    'guntur', 'darbhanga', 'saharanpur', 'panipat', 'bijapur', 'bellary',
    'durgapur', 'malappuram', 'navi mumbai', 'shimla'
]

def clean_location_string(location: str) -> str:
    """Clean the location string by removing special characters and extra spaces"""
    if not location:
        return ""
    # Remove special characters except comma and period
    cleaned = re.sub(r'[^\w\s,.]', '', str(location))
    # Replace multiple spaces with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    # Replace multiple commas with single comma
    cleaned = re.sub(r',+', ',', cleaned)
    return cleaned.strip()

def extract_location_components(location: str) -> Tuple[str, str, str]:
    """Extract city, state, and country from location string"""
    if not location or location.lower() == 'none' or location.lower() == 'null':
        return "", "", ""
    
    # Clean and lowercase the location string
    location = clean_location_string(location.lower())
    if not location:
        return "", "", ""
        
    parts = [part.strip() for part in location.split(',')]
    parts = [p for p in parts if p and p.lower() not in ['none', 'null']]
    
    city, state, country = "", "", ""
    
    # Check for country first
    if any('india' in p.lower() for p in parts):
        country = 'India'
        parts = [p for p in parts if 'india' not in p.lower()]
    
    # Check for state
    found_state = False
    for official_state, variations in INDIAN_STATES.items():
        for part in parts:
            if any(var in part.lower() for var in variations):
                state = official_state
                parts = [p for p in parts if not any(var in p.lower() for var in variations)]
                found_state = True
                break
        if found_state:
            break
    
    # Check for known cities
    found_city = False
    for part in parts:
        if any(city.lower() in part.lower() for city in MAJOR_CITIES):
            city = part.strip().title()
            parts.remove(part)
            found_city = True
            break
    
    # If no known city found, take first remaining part as city
    if not found_city and parts:
        city = parts[0].strip().title()
    
    return city, state, country or "India"

def update_coresignal_member_locations():
    """Update locations for CoreSignal members in the database"""
    try:
        # MongoDB connection
        client = pymongo.MongoClient(
            "CONNECTION_STRING",
            serverSelectionTimeoutMS=30000
        )
        db = client["PROD_STUDENT"] #DEV_STUDENT
        collection = db["students"]
        
        # Query to find CoreSignal members
        query = {
            "source": "coresignal",
            "address.location": {"$exists": True}
        }
        
        total_members = collection.count_documents(query)
        logger.info(f"Total CoreSignal members to process: {total_members}")
        
        batch_size = 100
        processed = 0
        updated = 0
        errors = 0
        
        cursor = collection.find(query, {"_id": 1, "address.location": 1})
        
        for student in cursor:
            try:
                student_id = student['_id']
                original_location = student.get('address', {}).get('location', '')
                
                city, state, country = extract_location_components(original_location)
                
                update_result = collection.update_one(
                    {"_id": student_id},
                    {
                        "$set": {
                            "address.city": city,
                            "address.state": state,
                            "address.country": country
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    updated += 1
                
                processed += 1
                
                # Log progress
                if processed % batch_size == 0:
                    logger.info(f"Processed {processed}/{total_members} records ({(processed/total_members)*100:.2f}%)")
                    logger.info(f"Successfully updated: {updated} records")
                
            except Exception as e:
                logger.error(f"Error processing student {student_id}: {str(e)}")
                errors += 1
                continue
        
        logger.info("\nProcessing Complete!")
        logger.info(f"Total records processed: {processed}")
        logger.info(f"Successfully updated: {updated}")
        logger.info(f"Errors encountered: {errors}")
        
        pipeline = [
            {"$match": {"source": "coresignal"}},
            {"$group": {"_id": "$address.state", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        state_stats = collection.aggregate(pipeline)
        
        logger.info("\nTop 10 States Distribution after update:")
        for stat in state_stats:
            logger.info(f"{stat['_id'] or 'No State'}: {stat['count']} records")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    
    finally:
        client.close()

if __name__ == "__main__":
    update_coresignal_member_locations()