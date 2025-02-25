import logging
import pandas as pd


def silver_to_gold(silver_path: str, gold_path: str):

    try:
        # Load data from silver
        logging.info(f"Reading silver data from {silver_path}")

        silver_df = pd.read_parquet(silver_path)
        deduplicated_silver = silver_df.drop_duplicates()

        # agg breweries by type and location
        logging.info("Creating aggregations by location")
        logging.info("Aggregating breweries by type and city")

        agg_breweries_type_city = (
            deduplicated_silver
            .groupby(["brewery_type", "city", "state_province"])
            .size() 
            .reset_index(name="brewery_count")
            .sort_values(by='brewery_count', ascending=False)
        )
        final_agg_breweries_type_city = agg_breweries_type_city[agg_breweries_type_city['brewery_count'] > 0]

        logging.info("Aggregating breweries by type and state")

        agg_breweries_type_state = (
            deduplicated_silver
            .groupby(["brewery_type", "state_province"])
            .size() 
            .reset_index(name="brewery_count")
            .sort_values(by='brewery_count', ascending=False)
        )
        final_agg_breweries_type_state = agg_breweries_type_state[agg_breweries_type_state['brewery_count'] > 0]

        # Save data
        logging.info(f"Saving aggregated data to {gold_path}")
        
        final_agg_breweries_type_city.to_parquet(gold_path + '/breweries_per_city.parquet', partition_cols=["city"])
        final_agg_breweries_type_state.to_parquet(gold_path + '/breweries_per_state.parquet', partition_cols=["state_province"])
        
        logging.info("Aggregations successfully saved to gold layer")
        
    except Exception as e:
        logging.error(f"Failed to create aggregations: {str(e)}")
        raise