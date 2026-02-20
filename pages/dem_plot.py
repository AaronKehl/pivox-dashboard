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

def plot_chart( tif_filename, color_label, color_scale, zmin="", zmax="" ):
    
    tif_data = read_idrive( 
        params["key_prefix"], params["bucket"], 
        params["owner"], params["site"], 
        params["dtype"], tif_filename 
    )
    tif_file = io.BytesIO( tif_data )

    with rio.open_rasterio( tif_file, masked=True ) as snowdepth:
        snowdepth = snowdepth.squeeze( "band", drop=True ) 
        if zmin != "" and zmax != "":
            fig = px.imshow( 
                snowdepth, color_continuous_scale = color_scale,
                title = tif_filename[:-3], origin = 'lower', 
                zmin = zmin, zmax = zmax, height = 500,
                labels = {"color":color_label}, aspect = "auto"
            )
        else:
            fig = px.imshow( 
                snowdepth, color_continuous_scale=color_scale,
                title=tif_filename[:-3], origin='lower', 
                height = 500,labels={"color":color_label}, aspect="auto"
            )
        fig.update_layout( 
            coloraxis_colorbar = {
                "thicknessmode":"pixels", "thickness":15,
                "lenmode":"pixels", "len":350,
                "title":"", "yanchor":"middle"
            },
            xaxis = {"automargin":True}, yaxis = {"automargin":True},
            #margin = {"r":100},
            #annotations=[{
            #    "text":color_label, "textangle":-90,
            #    "xref":"paper", "yref":"paper",
            #    "x":1.18, "y":0.5,
            #}] 
        )
        return fig

@st.cache_data
def plot_reg_dem_bare():
    tif_filename = params["tif_file"]
    color_label = "elevation [m]"
    color_scale = "rainbow_r"
    zmin = -0.2; zmax = 6.0
    fig = plot_chart( tif_filename, color_label, color_scale, zmin, zmax )
    st.plotly_chart( fig )

@st.cache_data
def plot_reg_dem_minmax():
    tif_filename = params["tif_file"]
    color_label = "elevation [m]"
    color_scale = "rainbow_r"
    zmin = ""; zmax = ""
    fig = plot_chart( tif_filename, color_label, color_scale, zmin, zmax )
    st.plotly_chart( fig )

@st.cache_data
def plot_sd_dem_bare():
    tif_filename = params["tif_file"][:-6] + "DEPTH.tif.gz"
    color_label = "snowdepth [m]"
    color_scale = "rainbow_r"
    zmin = -0.2; zmax = 6.0
    fig = plot_chart( tif_filename, color_label, color_scale, zmin, zmax )
    st.plotly_chart( fig )

@st.cache_data
def plot_sd_dem_minmax():
    tif_filename = params["tif_file"][:-6] + "DEPTH.tif.gz"
    color_label = "snowdepth [m]"
    color_scale = "rainbow_r"
    zmin = ""; zmax = ""
    fig = plot_chart( tif_filename, color_label, color_scale, zmin, zmax )
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
    tif_type = rad_left.radio( 
        " ", 
        ["Snowdepth DEM", "Regular DEM"], 
        label_visibility="collapsed", 
        horizontal=True 
    )
    tif_plot = btn_right.button( "Plot DEM", width="stretch" )
    
    if tif_type == "Regular DEM":
        if tif_color == "Show Colored by Min/Max of Individual Scan":
            plot_reg_dem_minmax(); zmin = ""; zmax = ""
        else: 
            plot_reg_dem_bare(); zmin = -0.2; zmax = 6.0

    if tif_type == "Snowdepth DEM":
        if tif_color == "Show Colored by Min/Max of Individual Scan":
            plot_sd_dem_minmax(); zmin = ""; zmax = ""
        else: 
            plot_sd_dem_bare(); zmin = -0.2; zmax = 6.0

    if tif_plot: 
        plot_chart.clear(); plot_chart( params, tif_type, tif_color, zmin, zmax )


