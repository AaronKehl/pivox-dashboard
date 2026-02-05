import csv, gzip, boto3, io
from datetime import datetime, timedelta
import streamlit as st
import pandas as pd

def read_idrive( key_prefix="idrive", bucket="pivox", owner="boise", site="freeman", dtype="" ):
    idrive = boto3.client( "s3", 
                     aws_access_key_id = st.secrets[key_prefix+"_key"], 
                     aws_secret_access_key = st.secrets[key_prefix+"_secret"],
                     endpoint_url = st.secrets[key_prefix+"_endpoint"],
                     )
    
    if owner == "":
        data = idrive.get_object( Bucket=bucket, Key=site+"/telemetry/"+site+"-master-"+dtype+".csv.gz" )
    else:
        data = idrive.get_object( Bucket=bucket, Key=owner+"/"+site+"/telemetry/"+site+"-master-"+dtype+".csv.gz" )
    contents = gzip.decompress( data['Body'].read() ).decode( 'iso8859_2' )
    return contents

def screen_data( variable, value ):
    # Add acceptable min/max for screening data based on var?
    try:
        if int( value ) != -9999: return True
    except: pass
    try: 
        if value == "0b0" or value == "0b1": return True
    except: pass
    try:
        if value == "0b00" or value == "0b01" \
            or value == "0b10" or value == "0b11":
            return True
    except: pass
    return False

def gen_chart_spec( y_names, y_units ):
    if len( y_names ) > 1: 
        return_json = {
            "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "encoding": {
                "x":{ "field":"Timestamp", "type":"temporal","axis":{"format":"%m/%d %H:%M","labelAngle":-90,"title":"Timestamp [mm/dd HH:MM]"}},
            },
            "layer": [{
                "mark":{"type": "line", "color": "#4682b4", "point": {"size":50,"filled":False,"color": "#4682b4" }},
                "encoding":{
                    "y":{ "field":y_names[0],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[0]+" ["+y_units[0]+"]", "titleColor": "#4682b4" }}
                },
                "layer":[{"mark":{ "type": "line", "color": "#4682b4", "point": {"size":50,"filled":False,"color": "#4682b4" }}},{"transform": [{ "filter":{ "param": "hover", "empty": False }}],"mark":"point" }],
            },
            {
                "mark":{"type": "line", "color": "#168b3d", "point": {"size":50,"filled":False,"color": "#168b3d" }},
                "encoding":{
                     "y":{ "field":y_names[1],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[1]+" ["+y_units[1]+"]", "titleColor": "#168b3d" }}
                },
                "layer":[{"mark":{ "type": "line", "color": "#168b3d", "point": {"size":50,"filled":False,"color": "#168b3d" }}},{"transform": [{ "filter":{ "param": "hover", "empty": False }}],"mark":"point" }],
            },
            {
                "transform": [{"field": y_names[0], "type": "quantitative" },{"field": y_names[1], "type": "quantitative" }],
                "mark": "rule",
                "encoding": { 
                    "opacity": { "condition": {"value": 0.3, "param":"hover", "empty": False }, "value":0 }, 
                    "tooltip": [ 
                        { "field": "Timestamp", "type": "temporal", "format":"%Y/%m/%d %H:%M:%S", "nearest": True },
                        { "field": y_names[0], "type": "quantitative" },
                        { "field": y_names[1], "type": "quantitative" }
                    ]
                },
                "params":[
                {
                    "name": "hover",
                    "select": {
                        "type": "point",
                        "fields": ["Timestamp"],
                        "nearest": True,
                        "on": "pointerover",
                        "clear": "pointerout"
                    }
                },
                {
                    "name": "grid",
                    "select": "interval",
                    "bind": "scales"
                }]
            }],
            "resolve": {"scale": {"y": "independent"}}
        }

    else: 
        return_json = {
            "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "encoding": {
                "x":{
                    "field":"Timestamp", 
                    "type":"temporal", 
                    "axis":{"format":"%m/%d %H:%M","labelAngle":-90,"title":"Timestamp [mm/dd HH:MM]"}
                }
            },
            "layer": [
                {
                    "encoding": { "y":{ "field":y_names[0],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[0]+" ["+y_units[0]+"]"} } },
                    "layer":[{"mark":{ "type": "line", "point": True }},{"transform": [{ "filter":{ "param": "hover", "empty": False }}], "mark": "point" }]
                },
                {
                    "transform": [{"field": y_names[0], "type": "quantitative" }],
                    "mark": "rule",
                    "encoding": { 
                        "opacity": { "condition": {"value": 0.3, "param":"hover", "empty": False }, "value":0 }, 
                        "tooltip": [ 
                            { "field": "Timestamp", "type": "temporal", "format":"%Y/%m/%d %H:%M:%S", "nearest": True },
                            { "field": y_names[0], "type": "quantitative" }
                        ]
                    },
                    "params":[
                        {
                            "name": "hover",
                            "select": {
                                "type": "point",
                               "fields": ["Timestamp"],
                               "nearest": True,
                               "on": "pointerover",
                               "clear": "pointerout"
                            }
                        },
                        {
                            "name": "grid",
                            "select": "interval",
                            "bind": "scales"
                        }
                    ]
                }
            ] 
        }
    return return_json

def plot_chart( data, plot_var_1="", plot_var_2="" , open_date="", close_date="" ):
    if open_date is None or open_date == "": open_date = datetime( 2020, 3, 9, 0, 0, 0)
    else: open_date = datetime( open_date.year, open_date.month, open_date.day, 0, 0, 0 )
    if close_date is None or close_date == "": close_date = datetime.now
    else: close_date = datetime( close_date.year, close_date.month, close_date.day, 23, 59, 59 )
    
    if plot_var_1 != "" or plot_var_2 != "":

        csvfile = io.StringIO( data )
        reader = csv.DictReader( csvfile, delimiter="," )
        var_names = reader.fieldnames
        x = []
        row_pos = 0
            
        if plot_var_1 != "" and plot_var_2 != "":
            y1=[]
            y2=[]
            for row in reader:
                if row_pos == 0:
                    x_units = row[ var_names[0] ] +" "+ row[ var_names[1] ]
                    y1_units = row[ plot_var_1 ]
                    y2_units = row[ plot_var_2 ]
                else:
                    file_time = row[ var_names[0] ] 
                    timestamp = datetime.strptime( file_time[:file_time.find(".")] , "%Y%m%d-%H%M-%S" )
                    if timestamp >= open_date and timestamp <= close_date:
                        try: y1_val = float( row[ plot_var_1 ] )
                        except: y1_val = row[ plot_var_1 ]
                        try: y2_val = float( row[ plot_var_2 ] )
                        except: y2_val = row[ plot_var_2 ]
                        if screen_data( row[ plot_var_1 ], y1_val ) \
                            and screen_data( row[ plot_var_2 ], y2_val ):
                            x.append( timestamp )
                            y1.append( y1_val )
                            y2.append( y2_val )

                row_pos += 1
            data = { "Timestamp": x, plot_var_1: y1, plot_var_2: y2 }
            data_frame = pd.DataFrame( data )
            #st.line_chart( data_frame, x="Timestamp", y=[plot_var_1, plot_var_2], height='stretch', width='stretch' )
            st.vega_lite_chart(
                data_frame,
                gen_chart_spec( [plot_var_1,plot_var_2], [y1_units,y2_units] ),
                width='stretch',
                height=500,
            )
            
        elif plot_var_1 != "" and plot_var_2 == "":
            y1=[]
            for row in reader:
                if row_pos == 0:
                    x_units = row[ var_names[0] ] +" "+ row[ var_names[1] ]
                    y1_units = row[ plot_var_1 ]
                else:
                    file_time = row[ var_names[0] ] 
                    timestamp = datetime.strptime( file_time[:file_time.find(".")] , "%Y%m%d-%H%M-%S" )
                    if timestamp >= open_date and timestamp <= close_date:
                        try: y1_val = float( row[ plot_var_1 ] )
                        except: y1_val = row[ plot_var_1 ]
                        if screen_data( row[ plot_var_1 ], y1_val ):
                            x.append( timestamp )
                            y1.append( y1_val )
                row_pos += 1
            data = { "Timestamp": x, plot_var_1: y1 }
            data_frame = pd.DataFrame( data )
            #st.line_chart( data_frame, x="Timestamp", y=plot_var_1, height='stretch', width='stretch' )
            spec = gen_chart_spec( [plot_var_1], [y1_units]  )
            print( spec )
            st.vega_lite_chart(
                data_frame,
                gen_chart_spec( [plot_var_1], [y1_units] ),
                width='stretch',
                height=500,
            )
            
        elif plot_var_2 != "" and plot_var_1 == "":
            y2=[]
            for row in reader:
                if row_pos == 0:
                    x_units = row[ var_names[0] ] +" "+ row[ var_names[1] ]
                    y2_units = row[ plot_var_2 ]
                else:
                    file_time = row[ var_names[0] ] 
                    timestamp = datetime.strptime( file_time[:file_time.find(".")] , "%Y%m%d-%H%M-%S" )
                    if timestamp >= open_date and timestamp <= close_date:
                        try: y2_val = float( row[ plot_var_2 ] )
                        except: y2_val = row[ plot_var_2 ]
                        if screen_data( row[ plot_var_2 ], y2_val ):
                            x.append( timestamp )
                            y2.append( y2_val )
                row_pos += 1
            data = { "Timestamp": x, plot_var_2: y2 }
            data_frame = pd.DataFrame( data )
            #st.line_chart( data_frame, x="Timestamp", y=plot_var_2, height='stretch', width='stretch' )
            st.vega_lite_chart(
                data_frame,
                gen_chart_spec( [plot_var_2], [y2_units] ),
                width='stretch',
                height=500,
            )
        
        else: pass

if __name__ == "__main__":
    params = st.query_params
    site_name = params["site"][0:1].upper() + params["site"][1:]

    # Set up the bones of the page
    st.set_page_config( page_title=site_name, page_icon="📈" )
    st.write("# " + site_name + " Pivox Z Level 📈")
    left_link, mid_link, right_link = st.columns(3)
    left_link.page_link( "dashboard.py", label="Dashboard" )
    mid_link.page_link( "pages/telemetry.py", label="Telemetry", query_params=params )
    right_link.page_link( "pages/images.py", label="Images", query_params=params )

    if params["dtype"] != "":
        # Grab z Level data from master file.
        data = read_idrive( params["key_prefix"], params["bucket"], params["owner"], params["site"], params["dtype"] )
        csvfile = io.StringIO( data )
        reader = csv.DictReader( csvfile, delimiter="," )
        var_names = reader.fieldnames

        # Set up options for graphing variables contained in the levels file by open/close dates
        top_left, top_right = st.columns( 2 )
        plot_var_1 = top_left.selectbox( "Select Variable 1", [""] + var_names )
        plot_var_2 = top_right.selectbox( "Select Variable 2", [""] + var_names )
        bot_left, bot_middle, bot_right = st.columns( 3, vertical_alignment = "bottom" )
        default_open = datetime.now() - timedelta( days = 7 )
        open_date = bot_left.date_input( "Begin Date", value = default_open )
        close_date = bot_middle.date_input( "End Date", value = "today" )
        if bot_right.button( "Plot Chart", use_container_width=True):
            plot_chart( data, plot_var_1, plot_var_2, open_date, close_date )

        # populate an image/description for the level page for a specific site
        st.image( "img/"+params["owner"]+"-"+params["site"]+"-"+params["dtype"]+".png", caption="Depiction of where level is determined." )
    else:
        st.write("## No additional telemetry file exists for this site, main telemetry file only." )
        
    #st.write( "You selected:", var_names )