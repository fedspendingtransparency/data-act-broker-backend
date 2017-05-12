from ftplib import FTP

# connect to host, default port
ftp = FTP('ftp.cfda.gov')

# anonymous FTP, the archive site allow general access
# user anonymous, password anonymous
ftp.login()

# change the directory to /usaspending/
ftp.cwd('usaspending')

data = []

# output the directory contents
ftp.dir('-t', data.append)

# get the most recent updated cfda file
file_listing = data[0]

# break string down by adding the data to a string array using a space separator
# example of the list: "22553023 May 07 01:51 programs-full-usaspending17126.csv"
file_parts = file_listing.split(' ')

# get the last string array (file name)
file_name = file_parts[-1]

print('Loading ' + file_name)

# use retrbinary() function to download file
ftp.retrbinary('RETR ' + file_name, open('dataactvalidator/config/cfda_program.csv', 'wb').write)

print('Loading completed')

ftp.quit()
