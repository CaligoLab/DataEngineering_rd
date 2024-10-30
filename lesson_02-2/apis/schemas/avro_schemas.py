PURCHASE_SCHEMA = {
        "type": "record",
        "name": "Purchase",
        "fields": [
            {
                "name": "client",
                "type": "string"
            },
            {
                "name": "purchase_date",
                "type": "string",
            },
            {
                "name": "product",
                "type": "string"
            },
            {
                "name": "price",
                "type": "int"
            }
        ]
}