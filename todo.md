## Todo list

- add download fallback on failure
  - option 1: each attempt tries a different format
  - option 2: after the last attempt, a new queue entry is made with different format
  - option 3: each attempt, each format is attempted (the download worker uses the fallback instead)
- update to psycopg3
- add title of work and author to download page title