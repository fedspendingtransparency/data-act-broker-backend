# Permissions

A simple guide covering permissions on the Broker and how they get set on each environment. 
For more details, please check out the code in [permissions.py](./permissions.py).

## Permission Levels

As listed out in [lookups.py](../dataactcore/models/lookups.py), there are five permission levels.
Each permission level must be granted via MAX groups which only apply to one agency at a time.
Users may be able to have permissions for multiple agencies which is only determined by the max groups
they are associated with.

**Note: For each group (DABS/FABS), each additional permission level builds upon the previous level 
(ex. DABS Writer can also read DABS Submissions).**

#### DABS (Data Act Broker Submissions)

1. **Reader (R)**: May read all aspects of submissions associated with the agency.
2. **Writer (W)**: May go through the submission process up to certifying (includes uploads, 
advancing in the submission, and deleting submissions).
3. **Submitter (S)**: May certify submissions for the agency.

#### FABS (Financial Assistance Broker Submissions)

1. **Edit-FABS (E)**: May upload files and delete submissions.
2. **FABS (F)**: May publish FABS submissions.

**Note: FABS users also have DABS reader permissions by default.** 

#### Administrators

Specific users may be given administrator access which includes all the access listed above.

## Translation from MAX Groups

Each time a user logs in, they go through [login.max.gov](https://portal.max.gov/home/sa/userHome). 
After authenticating, the Broker reads the group(s) the logged-in user is a part of and resets their Broker permissions.
These are determined by the MAX group name and agency associated with it.

### Max Groups

#### Components

- **Parent Group**: The MAX parent group that designates this is for the DATA-Act Broker. 
This can be used to separate permissions per environment (dev, staging, production).
- **CGAC**: The 3-digit code representing the toptier agency this permission is associated with.
- **FREC**: The 4-digit FR entity code representing the toptier agency this permission is associated with.
- **Permission Level**: The levels listed above with their letter code

#### Formats

- For CGAC agencies: **{Parent Group}-CGAC_{CGAC}-PERM_{Permission Level}**
- For FREC agencies: **{Parent Group}-CGAC_{CGAC}-FREC_{FREC}-PERM_{Permission Level}**

**Note: FREC permissions will give read access to the CGAC agency as well.**

For more details, check out [account_handler.py](./handlers/account_handler.py).
