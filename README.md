SPOTIFY API for Artist's Top Tracks


Simply add your client id's to a file called .env
Type in the name of the table at the top (and DB file name)
DB_FILE
TABLE_NAME

Type the name of the artist you're looking for in the main() method, and BOOM!
You get a table with the top hits for that artist.

Columns are renamed with "_" for example, "album_name". 
Rows are dropped, leaving just the necessary data remaining.
Column "preview_url" is empty on all data, so that was replaced with "Unknown".


"What tracks of theirs are acceptable to play at a coffee shop?"
All their non-explict tracks are returned in order to the terminal output, so you can check that there.
