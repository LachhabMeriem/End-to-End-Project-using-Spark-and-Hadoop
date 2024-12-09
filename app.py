import dash 
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
import pandas as pd
import plotly.graph_objs as go
from pyspark.sql import SparkSession
from os.path import abspath
import time

# Initialize Spark session with Hive support
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.scripts.config.serve_locally = True
current_refresh_time_temp = None

# Initial dataframe load from Spark
df = spark.sql("SELECT * FROM weather_table")
df_pd = df.toPandas()
# df_pd['index'] = range(1, len(df_pd) + 1)

PAGE_SIZE = 5

app.layout = html.Div([
    html.H2(
        children="Real-Time Dashboard for Weather Monitoring",
        style={
            "textAlign": "center",
            "color": "#4285F4",
            "font-weight": "bold",
            "font-family": "Verdana",
        },
    ),
    html.Div(
        children="{ A Idle Place for Knowing Your City's Weather}",
        style={
            "textAlign": "center",
            "color": "#0F9D58",
            "font-weight": "bold",
            "fontSize": 16,
            "font-family": "Verdana",
        },
    ),
    html.Br(),
    html.Div(
        id="current_refresh_time",
        children="Current Refresh Time: ",
        style={
            "textAlign": "center",
            "color": "black",
            "font-weight": "bold",
            "fontSize": 10,
            "font-family": "Verdana",
        },
    ),
    html.Div([
        html.Div([
            dcc.Graph(id='live-update-graph-bar')
        ], className="six columns"),

        html.Div([
            html.Br(), html.Br(), html.Br(), html.Br(),
            dash_table.DataTable(
                id='datatable-paging',
                columns=[{'name': i, "id": i} for i in sorted(df_pd.columns)],
                page_current=0,
                page_size=PAGE_SIZE,
                page_action='custom'
            )
        ], className='six columns'),
    ], className="row"),

    dcc.Interval(
        id="interval-component",
        interval=10000,  # Interval set to 10 seconds
        n_intervals=0,
    )
])

@app.callback(
    Output("current_refresh_time", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_layout(n):
    global current_refresh_time_temp
    current_refresh_time_temp = time.strftime("%Y-%m-%d  %H:%M:%S")
    return "Current Refresh Time: {}".format(current_refresh_time_temp)

@app.callback(
    Output("live-update-graph-bar", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph_bar(n):
    traces = []

    bar_1 = go.Bar(
        x=df_pd["cityname"].head(5),
        y=df_pd["temperature"].head(5),
        name="Temperature"
    )
    traces.append(bar_1)

    bar_2 = go.Bar(
        x=df_pd["cityname"].head(5),
        y=df_pd["humidity"].head(5),
        name="Humidity"
    )
    traces.append(bar_2)

    layout = go.Layout(
        barmode='group',
        title="City's Temperature and Humidity",
        titlefont=dict(
            family="Verdana",
            size=12,
            color="black"
        ),
        xaxis=dict(title='City'),
        yaxis=dict(title='Value')
    )

    return {'data': traces, 'layout': layout}

@app.callback(
    Output('datatable-paging', 'data'),
    [
        Input('datatable-paging', 'page_current'),
        Input('datatable-paging', 'page_size'),
        Input('interval-component', "n_intervals")
    ]
)
def update_table(page_current, page_size, n):
    # Reload the Spark DataFrame each time (you can optimize this if needed)
    df = spark.sql("SELECT * FROM weather_table")
    df_pd = df.toPandas()

    return df_pd.iloc[page_current * page_size:(page_current + 1) * page_size].to_dict('records')

if __name__ == "__main__":
    print("Starting Real-Time Dashboard for Weather Application ...")
    app.run_server(debug=True)
