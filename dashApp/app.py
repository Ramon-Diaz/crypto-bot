import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
import os

# Initialize the Dash app
app = dash.Dash(__name__, routes_pathname_prefix='/dash/')

def get_candlestick_data(currency):

    # Dictionary to store the connection parameters
    db_config = {
        'username': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'host': 'mysql',        
        'database': 'crypto_data',
        'port': 3306 # default MySQL port
    }

    connection_string = f"mysql+mysqlconnector://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)

    # Get today's date in 'YYYY-MM-DD' format
    today = datetime.now().strftime('%Y-%m-%d')

    # Write your SQL query to fetch only data from today
    query = f"SELECT * FROM {currency} WHERE DATE(datetime) = '{today}'"

    # Load the data into a Pandas DataFrame
    df = pd.read_sql(query, con=engine)

    # Display the DataFrame
    print(df)

    return df

# Define the layout of the app
app.layout = html.Div(style={'backgroundColor': '#111111', 'color': '#FFFFFF'}, children=[
    html.H1(children='Candlestick Chart Visualization', style={'textAlign': 'center', 'color': '#FFFFFF'}),
    dcc.Dropdown(
        id='currency-dropdown',
        options=[
            {'label': 'BTC/USD', 'value': 'BTCUSD'},
            {'label': 'EUR', 'value': 'EUR'},
            {'label': 'JPY', 'value': 'JPY'},
        ],
        value='BTCUSD',  # Default value
        style={'backgroundColor': '#333333', 'color': '#FFFFFF'}
    ),
    dcc.Graph(id='candlestick-graph'),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Refresh every 60 seconds (60 * 1000 milliseconds)
        n_intervals=0
    )
])

# Define the callback to update the candlestick chart
@app.callback(
    Output('candlestick-graph', 'figure'),
    Input('currency-dropdown', 'value'),
    Input('interval-component', 'n_intervals')
)
def update_graph(selected_currency, n):
    # Generate new dummy data for the selected currency
    df = get_candlestick_data(selected_currency)
    
    # Create a candlestick chart
    fig = go.Figure(data=[go.Candlestick(
        x=df['datetime'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close']
    )])

    # Update layout for dark theme
    fig.update_layout(
        plot_bgcolor='#111111',
        paper_bgcolor='#111111',
        font=dict(color='#FFFFFF'),
        title='Candlestick Chart for {}'.format(selected_currency),
        xaxis_title='Date',
        yaxis_title='Price',
        xaxis=dict(
            gridcolor='#333333',
            tickfont=dict(color='#FFFFFF')
        ),
        yaxis=dict(
            gridcolor='#333333',
            tickfont=dict(color='#FFFFFF')
        )
    )
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
