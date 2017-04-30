from ftplib import FTP

ftp = FTP('ftp.cfda.gov')      # connect to host, default port
ftp.login()                    # user anonymous, passwd anonymous@
ftp.cwd('usaspending')

data = []
ftp.dir('-t', data.append)
file_listing = data[0]
file_parts = file_listing.split(' ')
file_name = file_parts[-1]
print('Loading ' + file_name)

ftp.retrbinary('RETR ' + file_name, open('dataactvalidator/config/cfda_program.csv', 'wb').write)
print('Loading completed')
ftp.quit()
