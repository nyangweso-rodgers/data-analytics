with 
product_types as (
        SELECT id, 
        #old_id, 
        productType, 
        createdAt, 
        createdBy, 
        updatedAt, 
        updatedBy
        FROM amtdb.product_types
        )