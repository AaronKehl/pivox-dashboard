import gzip, boto3, io
import streamlit as st
import plotly.express as px
import rioxarray as rio

def read_idrive( key_prefix="idrive", bucket="pivox", owner="boise", site="freeman", dtype="", tif_file="" ):
    idrive = boto3.client( "s3", 
                     aws_access_key_id = st.secrets[key_prefix+"_key"], 
                     aws_secret_access_key = st.secrets[key_prefix+"_secret"],
                     endpoint_url = st.secrets[key_prefix+"_endpoint"],
                     )
    
    # here is where we grab the file contents to display on the page
    if owner == "": prefix = site + "/dems/"
    else: prefix = owner + "/" + site + "/dems/"
    
    data = idrive.get_object( Bucket=bucket, Key=prefix+tif_file )
    tif_data = gzip.decompress( data['Body'].read() )
    return tif_data

def gen_chart_spec( y_names, y_units ):
    if len( y_names ) > 1: 
        return_json = {
            "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "encoding": {
                "x":{ "field":"Timestamp", "type":"temporal","axis":{"format":"%m/%d %H:%M","labelAngle":-90,"title":"Timestamp [mm/dd HH:MM]"}},
            },
            "layer": [
            {
                "mark":{"type": "line", "color": "#4682b4", "point": {"size":10,"filled":False,"color": "#4682b4" }},
                "encoding":{
                    "y":{ "field":y_names[0],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[0]+" ["+y_units[0]+"]", "titleColor": "#4682b4" }},
                },
                "layer":[{"mark":{ "type": "line", "color": "#4682b4", "point": {"size":10,"filled":False,"color": "#4682b4" }}},{"transform": [{ "filter":{ "param": "hover", "empty": False }}],"mark":"point" }],
            },
            {
                "mark":{"type": "line", "color": "#168b3d", "point": {"size":10,"filled":False,"color": "#168b3d" }},
                "encoding":{
                    "y":{ "field":y_names[1],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[1]+" ["+y_units[1]+"]", "titleColor": "#168b3d" }}
                },
                "layer":[{"mark":{ "type": "line", "color": "#168b3d", "point": {"size":10,"filled":False,"color": "#168b3d" }}},{"transform": [{ "filter":{ "param": "hover", "empty": False }}],"mark":"point" }],
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
            "resolve":{ "scale":{"y":"independent"}}
        }

    else: 
        return_json = {
            "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "encoding": {
                "x":{"field":"Timestamp","type":"temporal","axis":{"format":"%m/%d %H:%M","labelAngle":-90,"title":"Timestamp [mm/dd HH:MM]"}},
                "y":{"field":y_names[0],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[0]+" ["+y_units[0]+"]"}}
            },
            "layer": [
            {
                "mark":{"type": "line", "point": {"size":10,"filled":False }},
                "encoding": { "y":{ "field":y_names[0],"type":"quantitative","scale":{"zero":False},"axis":{"title":y_names[0]+" ["+y_units[0]+"]"} } },
                "layer":[{"mark":{ "type": "line", "point": {"size":10, "filled":False} }},{"transform": [{ "filter":{ "param": "hover", "empty": False }}], "mark": "point" }]                },
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
                }]
            }] 
        }
    return return_json

def plot_chart( params, tif_type, tif_color ):
    if params["tif_file"] != "":
        # Grab z Level data from master file.
        # Sort out which tif file we are looking at
        if tif_type == "Snowdepth DEM":
            tif_filename = params["tif_file"][:-6] + "DEPTH.tif.gz"
        else: tif_filename = params["tif_file"]

        tif_data = read_idrive( params["key_prefix"], params["bucket"], params["owner"], params["site"], params["dtype"], tif_filename )
        tif_file = io.BytesIO( tif_data )

    with rio.open_rasterio( tif_file, masked=True ) as snowdepth:
        snowdepth = snowdepth.squeeze( "band", drop=True )
        if tif_color == "Show Colored by Min/Max of Individual Scan":
            fig = px.imshow( 
                snowdepth, 
                color_continuous_scale="rainbow_r",
                title=tif_filename[:-3], 
                origin='lower' )
        else: 
            fig = px.imshow( 
                snowdepth, 
                color_continuous_scale="rainbow_r",
                title=tif_filename[:-3], 
                origin='lower',
                zmin=-0.2,
                zmax=6.0 )
        st.plotly_chart( fig )

if __name__ == "__main__":
    params = st.query_params
    site_name = params["site"][0:1].upper() + params["site"][1:]

    # Set up the bones of the page
    st.set_page_config( page_title=site_name, page_icon="🏔" )
    st.write("# " + site_name + " Pivox DEM Plot 🏔")
    link_left, link_midl, link_midr, link_right = st.columns(4)
    link_left.page_link( "pages/dems.py", label="Back", query_params=params )

    # Radio buttons for which type we want to view...
    #rad_left, rad_right = st.columns( 2 )
    tif_color = st.radio( " ", 
                            ["Show Colored by Min/Max of Individual Scan", 
                             "Show Colored by Reference to Bare Earth"],
                             label_visibility="collapsed",
                             horizontal=True
                            )

    rad_left, btn_right = st.columns( 2 )
    tif_type = rad_left.radio( " ", ["Regular DEM", "Snowdepth DEM"], label_visibility="collapsed", horizontal=True)
    tif_plot = btn_right.button( "Plot DEM", width="stretch" )
    
    if tif_plot: plot_chart( params, tif_type, tif_color )

