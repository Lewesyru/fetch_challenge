import json
import pandas as pd
import sqlalchemy as sa

# Load JSON file
with open("raw/receipts.json", "r") as f:
    receipts = json.load(f)
with open("raw/brands.json", "r") as f:
    brands = json.load(f)
with open("raw/users.json", "r") as f:
    users = json.load(f)

# Function to extract MongoDB ObjectId
def extract_oid(obj):
    """Extracts the ObjectId from MongoDB-style '_id' fields."""
    return obj["$oid"] if isinstance(obj, dict) and "$oid" in obj else obj

# Function to convert MongoDB Unix timestamp to datetime
def convert_timestamp(obj):
    """Converts MongoDB-style '$date' timestamps into pandas datetime."""
    return pd.to_datetime(obj["$date"], unit='ms') if isinstance(obj, dict) and "$date" in obj else obj

# initialize lists to store data
receipts_list = []
items_list = []
brands_list = []
users_list = []

for receipt in receipts:
    # Extract top-level fields
    receipt_dict = {
        "_id": extract_oid(receipt["_id"]),
        "user_id": receipt.get("userId"),
        "bonus_points_earned": receipt.get("bonusPointsEarned"),
        "bonus_points_earned_reason": receipt.get("bonusPointsEarnedReason"),
        "create_date": convert_timestamp(receipt.get("createDate")),
        "date_scanned": convert_timestamp(receipt.get("dateScanned")),
        "finished_date": convert_timestamp(receipt.get("finishedDate")),
        "modify_date": convert_timestamp(receipt.get("modifyDate")),
        "points_awarded_date": convert_timestamp(receipt.get("pointsAwardedDate")),
        "points_earned": float(receipt["pointsEarned"]) if receipt.get("pointsEarned") else None,
        "purchase_date": convert_timestamp(receipt.get("purchaseDate")),
        "purchased_item_count": receipt.get("purchasedItemCount"),
        "rewards_receipt_status": receipt.get("rewardsReceiptStatus"),
        "total_spent": float(receipt["totalSpent"]) if receipt.get("totalSpent") else None,
    }
    receipts_list.append(receipt_dict)

    # Process nested `rewardsReceiptItemList`
    if "rewardsReceiptItemList" in receipt and isinstance(receipt["rewardsReceiptItemList"], list):
        for item in receipt["rewardsReceiptItemList"]:
            item_dict = {
                "receipt_id": receipt_dict["_id"],  # Foreign Key
                "barcode": item.get("barcode"),
                "description": item.get("description"),
                "final_price": float(item["finalPrice"]) if item.get("finalPrice") else None,
                "item_price": float(item["itemPrice"]) if item.get("itemPrice") else None,
                "needs_fetch_review": item.get("needsFetchReview"),
                "partner_item_id": item.get("partnerItemId"),
                "prevent_target_gap_points": item.get("preventTargetGapPoints"),
                "quantity_purchased": item.get("quantityPurchased"),
                "user_flagged_barcode": item.get("userFlaggedBarcode"),
                "user_flagged_new_item": item.get("userFlaggedNewItem"),
                "user_flagged_price": float(item["userFlaggedPrice"]) if item.get("userFlaggedPrice") else None,
                "user_flagged_quantity": item.get("userFlaggedQuantity"),
                "points_earned": float(item["pointsEarned"]) if item.get("pointsEarned") else None,
                "points_payer_id": item.get("pointsPayerId"),
                "rewards_group": item.get("rewardsGroup"),
                "rewards_product_partner_id": item.get("rewardsProductPartnerId"),
                "target_price": float(item["targetPrice"]) if item.get("targetPrice") else None,
                "competitive_product": item.get("competitiveProduct"),
                "discounted_item_price": float(item["discountedItemPrice"]) if item.get("discountedItemPrice") else None,
                "original_receipt_item_text": item.get("originalReceiptItemText"),
                "price_after_coupon": float(item["priceAfterCoupon"]) if item.get("priceAfterCoupon") else None,
                "brand_code": item.get("brandCode"),
                "points_not_awarded_reason": item.get("pointsNotAwardedReason"),
                "needs_fetch_review_reason": item.get("needsFetchReviewReason"),
                "user_flagged_description": item.get("userFlaggedDescription"),
                "original_meta_brite_barcode": item.get("originalMetaBriteBarcode"),
                "deleted": item.get("deleted"),
                "original_meta_brite_description": item.get("originalMetaBriteDescription"),
                "competitor_rewards_group": item.get("competitorRewardsGroup"),
                "item_number": item.get("itemNumber"),
                "original_meta_brite_quantity_purchased": item.get("originalMetaBriteQuantityPurchased"),                
            }
            items_list.append(item_dict)

for brand in brands:
    brand_dict = {
        "_id": extract_oid(brand["_id"]),
        "barcode": brand.get("barcode"),
        "brand_code": brand.get("brandCode"),
        "category": brand.get("category"),
        "category_code": brand.get("categoryCode"),
        "cpg_ref": brand["cpg"]["$ref"],
        "cpg_id": extract_oid(brand["cpg"]["$id"]),
        "top_brand": brand.get("topBrand"),
        "name": brand.get("name"),
    }
    brands_list.append(brand_dict)

for user in users:
    user_dict = {
        "_id": extract_oid(user["_id"]),
        "state": user.get("state"),
        "created_date": convert_timestamp(user.get("createdDate")),
        "last_login": convert_timestamp(user.get("lastLogin")),
        "role": user.get("role"),
        "active": user.get("active"),
    }
    users_list.append(user_dict)


# Convert to DataFrames
df_receipts = pd.DataFrame(receipts_list)
df_items = pd.DataFrame(items_list)
df_brands = pd.DataFrame(brands_list)
df_users = pd.DataFrame(users_list)


# Connect to PostgreSQL
engine = sa.create_engine("postgresql://{user}:{password}@localhost:5432/{database}".format(
    user="yiru",
    password="",
    database="postgres"))

# Load to SQL
df_receipts.to_sql("receipts", engine, schema='yiru', if_exists="replace", index=False)
df_items.index += 1
df_items.to_sql("receipt_items", engine, schema='yiru', if_exists="replace", index=True, index_label="_id")
df_brands.to_sql("brands", engine, schema='yiru', if_exists="replace", index=False)
df_users.to_sql("users", engine, schema='yiru', if_exists="replace", index=False)
