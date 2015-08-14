# Zero
Zero keeps track of what happens in the lobby.

Specifically, the lobbies of the House and Senate.

Use Zero from an interactive session, for e.g. iPython.

To, for example, get all lobbying records for 2015 and put them in a subdirectory of an existing directory called data:

`from zero import zero as z`

`z.SOPRDownloader('2015','data')`

To parse through all of the results and get back a pandas dataframe (this is a bit slow):

`df = z.build_year('data/2015').results`

