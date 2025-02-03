import streamlit as st
from datetime import datetime, timedelta
import boto3
import io
import cv2
import numpy as np
from PIL import Image
import base64

def read_idrive( open_date="", close_date="" ):
    # fix date range to search
    if open_date is None or open_date == "": open_date = datetime( 2020, 3, 9, 0, 0, 0)
    else: open_date = datetime( open_date.year, open_date.month, open_date.day, 0, 0, 0 )
    if close_date is None or close_date == "": close_date = datetime.now
    else: close_date = datetime( close_date.year, close_date.month, close_date.day, 23, 59, 59 )

    # generate client session to cloud server
    idrive = boto3.client( "s3", 
                     aws_access_key_id = st.secrets["key"], 
                     aws_secret_access_key = st.secrets["secret"],
                     endpoint_url = st.secrets["endpoint"],
                     )

    # grab photo names from bucket, but parse down to between open/close dates
    image_show = []
    for image in idrive.list_objects( Bucket="pivox", Prefix="freeman/photos/" )["Contents"]:
        try:
            filename = image["Key"].strip( "freeman/photos/" )
            timestamp = datetime.strptime( filename[:filename.find(".")], '%Y%m%d-%H%M-%S' )
            if timestamp >= open_date and timestamp <= close_date: image_show.append( filename )
        except: pass
    
   # images=[]
    #fo#r image_name in image_show:
      #  data = idrive.get_object( Bucket="pivox", Key="freeman/photos/"+image_show[0] )
     #   image_data = io.BytesIO( data['Body'].read() )
    #    images.append( Image.open( image_data ) )

    #gif_buffer = io.BytesIO()
    #images[0].save(
    #    gif_buffer,
    #    save_all=True,
    #    append_images=images[1:],
    #    optimize=False,
    #    duration=200,
    #    loop=0
    #)

    #"""### gif from local file"""
    #gif_buffer_mkdown = base64.b64encode( gif_buffer ).decode("utf-8")

    #st.markdown(
    #    f'<img src="data:{gif_buffer_mkdown};base64,{gif_buffer_mkdown}" alt="cat gif">',
    #    unsafe_allow_html=True,
    #)

    rows = []
    row_data = []
    full_rows = int( len( image_show ) / 3 )
    part_row = int( len( image_show ) % 3 )
    for i in range( full_rows ): rows.append( st.columns( 3 ) )
    if part_row != 0: rows.append( st.columns( part_row ) )
    for entry in rows: row_data = row_data + entry

    # here is where we grab the file contents to display on the page
    index = 0
    for col in row_data:
        tile = col.container(height=180)
        data = idrive.get_object( Bucket="pivox", Key="freeman/photos/"+image_show[index] )
        image_file = data['Body'].read()
        caption = image_show[index][:image_show[index].find(".")]
        tile.image( image_file, caption )
        index += 1

st.set_page_config( page_title="Freeman", page_icon="👋" )
st.write("# Freeman Pivox, Idaho City, ID")
link_left, link_mid, link_right = st.columns( 3 )
link_left.page_link( "dashboard.py", label="Dashboard" )
link_mid.page_link( "pages/freeman.py" , label="Telemetry" )

bot_left, bot_middle, bot_right = st.columns( 3, vertical_alignment = "bottom" )
default_open = datetime.now() - timedelta( days = 7 )
open_date = bot_left.date_input( "Begin Date", value = default_open )
close_date = bot_middle.date_input( "End Date", value = "today" )
#read_idrive( open_date, close_date )

if bot_right.button( "Show Images", use_container_width=True):
    read_idrive( open_date, close_date )