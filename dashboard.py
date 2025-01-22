import streamlit as st

# Station details here
stations = [ "Freeman", "Nebraska", "USGS1", "Surrydam", "CEATS", "MICH" ]

st.set_page_config( page_title="dashboard", page_icon="👋" )
st.write("# Pivox Systems Dashboard 👋")

rows = []
row_data = []
full_rows = int( len( stations ) / 3 )
part_row = int( len( stations ) % 3 )
for i in range( full_rows ): rows.append( st.columns( 3 ) )
if part_row != 0: rows.append( st.columns( part_row ) )
for entry in rows: row_data = row_data + entry

index = 0
for col in row_data:
    unit = stations[index]
    tile = col.container(height=180)
    tile.page_link( "pages/"+stations[index].lower()+".py", label=stations[index] )
    tile.image( "img/"+stations[index].lower()+".png" )
    index += 1