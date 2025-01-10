class SqlQueries:

    """
        This class can also be run for the rest of the tables (this is why i set the table id), 
        but we assume that previous data checks are made before data comes into GCS 
        (plus there may not be constraints for null values that we would've check erroneously),
        depending on the specific scenario.
    """

    def __init__(self, column_id, project_id, dataset_id, table):
        self.column_id = column_id
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table = table
        self.complete_table_name = f'{project_id}.{dataset_id}.{table}'

    def row_count(self):
        return f"""
            SELECT COUNT(*) AS row_count FROM {self.complete_table_name};
"""
    row_count_cb_without_view = """
            SELECT COUNT(*) AS row_count FROM `potent-app-439210-c8.radusStore.customer_behaviour`
            WHERE event_type <> 'view';
"""

    def uniqueness_check(self):
        return f"""
            SELECT {self.column_id}, COUNT(*) AS duplicate_count FROM {self.complete_table_name}
            GROUP BY {self.column_id}
            HAVING  duplicate_count > 1;
"""
    uniqueness_check_fact = """
        SELECT 
            COUNT(*) AS duplicate_count,
            product_id, 
            customer_id, 
            product_name, 
            product_category, 
            interaction_type, 
            was_ordered, 
            rating, 
            customer_country, 
            timestamp
        FROM 
            `potent-app-439210-c8.radusStore.fact_product_popularity`
        GROUP BY 
            product_id, 
            customer_id, 
            product_name, 
            product_category, 
            interaction_type, 
            was_ordered, 
            rating, 
            customer_country, 
            timestamp
        HAVING 
            duplicate_count > 1;
        """
    
    def check_null_value(self, column_name):
        return f"""
            SELECT COUNTIF({column_name} IS NULL)  as null_count
            FROM {self.complete_table_name}
    """
        
    def rating_validation(self):
        return f"""
            SELECT MIN(rating) AS min_rating, MAX(rating) AS max_rating
            FROM {self.complete_table_name}
            WHERE rating IS NOT NULL
"""

    product_id_check = """
        SELECT fp.product_id 
        FROM `potent-app-439210-c8.radusStore.fact_product_popularity` fp
        LEFT JOIN `potent-app-439210-c8.radusStore.products` p ON fp.product_id = p.product_id
        WHERE p.product_id IS NULL;
"""

    customer_id_check = f"""
        SELECT fp.customer_id 
        FROM `potent-app-439210-c8.radusStore.fact_product_popularity` fp
        LEFT JOIN `potent-app-439210-c8.radusStore.customers` c ON fp.customer_id = c.customer_id
        WHERE c.customer_id IS NULL;
"""

    def timestamp_validity_check(self):
        return f""" 
            SELECT COUNT(*) AS invalid_dates
            FROM {self.complete_table_name}
            WHERE timestamp > CURRENT_TIMESTAMP()
"""

