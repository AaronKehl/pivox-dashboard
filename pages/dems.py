
import streamlit as st
from datetime import datetime, timedelta
import boto3

def adjust_params( params, tif_file ):
    tif_file = tif_file[:tif_file.find("DEPTH")] + "tif.gz"
    params.update( { "tif_file":tif_file } )
    return params

def read_idrive( open_date="", close_date="", key_prefix="idrive", bucket="pivox", owner="boise", site="freeman", dtype="" ):
    # fix date range to search
    if open_date is None or open_date == "": open_date = datetime( 2020, 3, 9, 0, 0, 0)
    else: open_date = datetime( open_date.year, open_date.month, open_date.day, 0, 0, 0 )
    if close_date is None or close_date == "": close_date = datetime.now
    else: close_date = datetime( close_date.year, close_date.month, close_date.day, 23, 59, 59 )

    # generate client session to cloud server
    idrive = boto3.client( "s3", 
                     aws_access_key_id = st.secrets[key_prefix+"_key"], 
                     aws_secret_access_key = st.secrets[key_prefix+"_secret"],
                     endpoint_url = st.secrets[key_prefix+"_endpoint"],
                     )

    # grab photo names from bucket, but parse down to between open/close dates first 1000 files.
    image_show = []
    if owner == "": prefix = site + "/dems/"
    else: prefix = owner + "/" + site + "/dems/"
    images = idrive.list_objects_v2( Bucket=bucket, Prefix=prefix)
    for image in images["Contents"]:
        try:
            filename = image["Key"].replace( prefix, "" )
            timestamp = datetime.strptime( filename[:filename.find(".")], '%Y%m%d-%H%M-%S' )
            if timestamp >= open_date and timestamp <= close_date and ".png" in filename: 
                if minmax_color:
                    if "IND" in filename: image_show.append( filename )
                if barearth_color:
                    if "ABS" in filename: image_show.append( filename )
                #print( filename )
        except: pass
            #idrive.delete_object( Bucket="pivox", Key="/boise/freeman/photos/"+"", IfMatchSize=0 )
            #print( image[ "ETag" ] )
    
    # loop through listings if > 1000 files
    while images['IsTruncated']:
        images = idrive.list_objects_v2( 
                        Bucket=bucket, 
                        Prefix=prefix, 
                        ContinuationToken=images['NextContinuationToken'] 
                    )
        for image in images["Contents"]:
            try:
                filename = image["Key"].replace( prefix, "" )
                timestamp = datetime.strptime( filename[:filename.find(".")], '%Y%m%d-%H%M-%S' )
                if timestamp >= open_date and timestamp <= close_date: image_show.append( filename )
            except: pass

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
        tile = col.container(height=185, vertical_alignment="top",horizontal_alignment="center", gap="xxsmall")
        data = idrive.get_object( Bucket=bucket, Key=prefix+image_show[index] )
        image_file = data['Body'].read()
        image_name = image_show[index] 
        caption = image_name[:image_name.find(".")]
        tile.page_link( "pages/dem_plot.py", label=caption, query_params=adjust_params( params, image_name ) )
        tile.image( image_file )
        index += 1


if __name__ == "__main__":
    params = st.query_params
    site_name = params["site"][0:1].upper() + params["site"][1:]
    
    st.set_page_config( page_title=site_name, page_icon="⛰" )
    st.write("# " + site_name + " Pivox DEMs ⛰")
    link_left, link_midl, link_midr, link_right = st.columns( 4 )
    link_left.page_link( "dashboard.py", label="Dashboard" )
    link_midl.page_link( "pages/telemetry.py" , label="Telemetry", query_params=params )
    link_midr.page_link( "pages/images.py", label="Images", query_params=params  )
    link_right.page_link( "pages/levels.py", label="Z Level", query_params=params )

    bot_left, bot_middle, bot_right = st.columns( 3, vertical_alignment = "bottom" )
    default_open = datetime.now() - timedelta( days = 7 )
    open_date = bot_left.date_input( "Begin Date", value = default_open )
    close_date = bot_middle.date_input( "End Date", value = "today" )

    # Radio buttons for which type we want to view...
    tgl_left, tgl_right = st.columns( 2 )
    minmax_color = tgl_left.toggle( "Show Colored by Min/Max of Individual Scan" )
    barearth_color = tgl_right.toggle( "Show Colored by Reference to Bare Earth" )
    
    st.caption( "Click image title to plot the raster on a chart.", text_alignment='center' )

    if bot_right.button( "Show Images", use_container_width=True ):
        read_idrive( open_date, close_date, params["key_prefix"], params["bucket"], params["owner"], params["site"], params["dtype"] )