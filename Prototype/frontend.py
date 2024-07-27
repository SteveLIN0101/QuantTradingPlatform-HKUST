import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Output, Input
import random

app = dash.Dash(__name__)

# Set the paths to the text files
file_paths = {
    'Executed Orders': 'executed_orders.txt',
    'Latest Market Data': 'market_data_output.txt',
}

def get_newest_line(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        if lines:
            return lines[-1].strip()
        else:
            return 'No data available'

strategies = ['Strategy A', 'Strategy B', 'Strategy C']

app.layout = html.Div(style={'textAlign': 'center', 'padding': '20px', 'fontFamily': 'Arial, sans-serif'}, children=[
    html.H1('Latest Updates', style={'color': '#007BFF'}),
    
    html.Div(style={'margin': '20px'}, children=[
        html.H2('Executed Orders', style={'color': '#333'}),
        html.Div(id='executed-orders', style={'fontSize': '20px', 'color': '#333'}),
    ]),
    
    html.Div(style={'margin': '20px'}, children=[
        html.H2('Latest Market Data', style={'color': '#333'}),
        html.Div(id='market-data', style={'fontSize': '20px', 'color': '#333'}),
    ]),
    
    html.Div(style={'margin': '20px'}, children=[
        html.H2('Sample Profit and Loss', style={'color': '#333'}),
        html.Div(id='profit-loss', style={'fontSize': '20px', 'color': '#333'}),
    ]),
    
    html.Div(style={'margin': '20px'}, children=[
        html.Label('Select Strategy:', style={'fontSize': '18px'}),
        dcc.Dropdown(
            id='strategy-dropdown',
            options=[{'label': strategy, 'value': strategy} for strategy in strategies],
            value=strategies[0]
        ),
    ]),
    
    dcc.Interval(
        id='interval-component',
        interval=1000,  # Update every 1 second
        n_intervals=0
    )
])

@app.callback(
    Output('executed-orders', 'children'),
    Output('market-data', 'children'),
    Output('profit-loss', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_content(n):
    executed_order = get_newest_line(file_paths['Executed Orders'])
    market_data = get_newest_line(file_paths['Latest Market Data'])
    profit_loss = f'Profit/Loss: ${random.uniform(-1000, 1000):.2f}'
    
    return executed_order, market_data, profit_loss

if __name__ == '__main__':
    app.run_server(debug=True)