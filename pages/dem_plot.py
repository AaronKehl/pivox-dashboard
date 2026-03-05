import gzip, boto3, io, json
import streamlit as st
import plotly.express as px
import rioxarray as rio
import rasterio
import rasterio.fill
import xarray as xr
from datetime import datetime

default_chart_spec = {
    "units":"[]",
    "xlabel":"X",
    "ylabel":"Y",
    "interp_val":6,
    "smooth_val":0,
    "zmin":-0.2,
    "zmax":6.0,
    "cmap":"rainbow_r"
}

def read_idrive( key_prefix="idrive", bucket="pivox", owner="boise", site="freeman", dtype="", tif_file="", chart=False ):
    idrive = boto3.client( "s3", 
                     aws_access_key_id = st.secrets[key_prefix+"_key"], 
                     aws_secret_access_key = st.secrets[key_prefix+"_secret"],
                     endpoint_url = st.secrets[key_prefix+"_endpoint"],
                     )
    
    if chart:
        if owner == "": prefix = site + "/" + site
        else: prefix = owner + "/" + site + "/" + site
        try: 
            data = idrive.get_object( Bucket=bucket, Key=prefix+"-chart-spec.json" )
            chart_spec = json.loads( data['Body'].read() )
            return chart_spec
        except:
            print( "[DEM_PLOT]: There was an issue processing chart_spec.json in the site folder." )
            return default_chart_spec

    # here is where we grab the file contents to display on the page
    else:
        if owner == "": prefix = site + "/dems/"
        else: prefix = owner + "/" + site + "/dems/"
        data = idrive.get_object( Bucket=bucket, Key=prefix+tif_file )
        tif_data = gzip.decompress( data['Body'].read() )
        return tif_data

def plot_chart( tif_filename, color_label, zvals = False ):
    chart_spec = read_idrive(
        params["key_prefix"], params["bucket"], 
        params["owner"], params["site"], 
        params["dtype"], tif_filename, chart=True
    )
    color_label = color_label + " " + chart_spec["units"]

    tif_data = read_idrive( 
        params["key_prefix"], params["bucket"], 
        params["owner"], params["site"], 
        params["dtype"], tif_filename 
    )
    tif_file = io.BytesIO( tif_data )

    with rasterio.open( tif_file ) as snowdepth:
        sd_mask = snowdepth.read_masks( 1 )
    with rio.open_rasterio( tif_file, masked=True ) as snowdepth:
        snowdepth = snowdepth.squeeze( "band", drop=True )
        if chart_spec["interp_val"] != 0:
            vals_interp = rasterio.fill.fillnodata( 
                snowdepth, 
                mask=sd_mask, 
                max_search_distance = chart_spec["interp_val"],
                smoothing_iterations = chart_spec["smooth_val"] 
            )
            snowdepth = xr.DataArray( 
                vals_interp, 
                coords = {
                    'y':snowdepth.y,
                    'x':snowdepth.x
                },
                dims=('y','x') 
            )

    if zvals:
        fig = px.imshow( 
            snowdepth, color_continuous_scale = chart_spec["cmap"],
            origin = 'lower', 
            zmin = chart_spec["zmin"], zmax = chart_spec["zmax"],
            labels = {
                "x":chart_spec["xlabel"],
                "y":chart_spec["ylabel"],
                "color":color_label
            }, 
            aspect = "auto",
            height = 500
        )
    else:
        fig = px.imshow( 
            snowdepth, color_continuous_scale = chart_spec["cmap"],
            origin = 'lower', 
            labels = {
                "x":chart_spec["xlabel"],
                "y":chart_spec["ylabel"],
                "color":color_label
            }, 
            aspect = "auto",
            height = 500,
        )
    fig.update_layout( 
        coloraxis_colorbar = {
            "thicknessmode":"pixels", "thickness":15,
            "lenmode":"pixels", "len":382,
            "title":"", 
            "yanchor":"top", "y":1.03,
            "xanchor":"left","x":0.97
        },
        xaxis = {"automargin":True}, 
        yaxis = {"automargin":True},
        margin = {"r":100},
        annotations=[{
            "text":color_label, "textangle":-90,
            "xref":"paper", "yref":"paper",
            "x":1.18, "y":0.45,
        }],
        title = tif_filename[:-3],
        title_font = {
            "size":14,
        },
        title_xanchor = 'center', title_x = 0.49
    )
    return fig

@st.cache_data
def plot_reg_dem_bare():
    tif_filename = params["tif_file"]
    color_label = "elevation"
    fig = plot_chart( tif_filename, color_label, zvals = True )
    st.plotly_chart( fig )

@st.cache_data
def plot_reg_dem_minmax():
    tif_filename = params["tif_file"]
    color_label = "elevation"
    fig = plot_chart( tif_filename, color_label )
    st.plotly_chart( fig )

@st.cache_data
def plot_sd_dem_bare():
    tif_filename = params["tif_file"][:-6] + "DEPTH.tif.gz"
    color_label = "snowdepth"
    fig = plot_chart( tif_filename, color_label, zvals = True )
    st.plotly_chart( fig )

@st.cache_data
def plot_sd_dem_minmax():
    tif_filename = params["tif_file"][:-6] + "DEPTH.tif.gz"
    color_label = "snowdepth"
    fig = plot_chart( tif_filename, color_label )
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
        [   "Show Colored by Reference to Bare Earth",
            "Show Colored by Min/Max of Individual Scan"],
        label_visibility="collapsed",
        horizontal=True
    )

    rad_left, btn_right = st.columns( 2 )
    tif_type = rad_left.radio( 
        " ", 
        ["Snowdepth DEM", "Regular DEM"], 
        label_visibility="collapsed", 
        horizontal=True 
    )
    tif_plot = btn_right.button( "Clear Cache", width="stretch" )
    
    if tif_type == "Regular DEM":
        if tif_color == "Show Colored by Min/Max of Individual Scan":
            plot_reg_dem_minmax()
        else: plot_reg_dem_bare()

    if tif_type == "Snowdepth DEM":
        if tif_color == "Show Colored by Min/Max of Individual Scan":
            plot_sd_dem_minmax()
        else: plot_sd_dem_bare()

    if tif_plot: 
        plot_sd_dem_bare.clear(); plot_sd_dem_minmax.clear()
        plot_reg_dem_bare.clear(); plot_reg_dem_minmax.clear()
