import geopandas as gpd
import h3pandas

if __name__ == '__main__':
    df = gpd.read_file("../../data/taxi_zones.geojson")
    gdf = df.h3.polyfill(10, explode=True)
    gdf.to_parquet("../../data/nyc_taxi_zones_h3_10.parquet.gzip", compression='gzip', index=False)
