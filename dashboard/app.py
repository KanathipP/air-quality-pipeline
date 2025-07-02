import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import duckdb
import pandas as pd

with duckdb.connect("../air_quality.db", read_only=True) as db_connection:
        df = db_connection.execute(
            "SELECT * FROM presentation.daily_air_quality_stats"
        ).fetchdf()
        
        daily_stats_df = db_connection.execute(
            "SELECT * FROM presentation.daily_air_quality_stats"
        ).fetchdf()
        
        latest_values_df = db_connection.execute(
            "SELECT * FROM presentation.latest_param_values_per_location"
        ).fetchdf()
        
def map_figure():
    map_fig = px.scatter_mapbox(
        latest_values_df,
        lat="lat",
        lon="lon",
        hover_name="location",
        hover_data={
            "lat":True,
            "lon":True,
            "datetime":True,
            "pm10":True,
            "pm25":True,
            "so2":True,
        },
        zoom=6.0
    )
    
    map_fig.update_layout(
        mapbox_style="open-street-map",
        height=800,
        title="Air Quality Monitoring Locations"
    )
    
    return map_fig

def line_figure():
    line_fig = px.line(
        daily_stats_df[daily_stats_df["parameter"] == "so2"].sort_values(by="measurement_date"),
        x="measurement_date",
        y="average_value",
        title="Plot over time of SO2 Levels"
    )
    
    return line_fig

def box_figure():
    box_fig = px.box(
        daily_stats_df[daily_stats_df["parameter"] == "so2"].sort_values(by="weekday_number"),
        x="weekday",
        y="average_value",
        title="Distribution of SO2 by Weekday"
    )
    
    return box_fig
    
app = dash.Dash(__name__)

app_layout = html.Div([
   dcc.Tabs([
       dcc.Tab(
           label="Sensor Locations",
           children = dcc.Graph(id="map-view", figure=map_figure())
        ),
        dcc.Tab(
           label="Parameter Plots",
           children = [
               dcc.Graph(id="line-plot", figure=line_figure()),
               dcc.Graph(id="box-plot", figure=box_figure()),
            ]
        ),
       
       ]) 
])

if __name__ == "__main__":
    app.run_server(debug=True)
    
