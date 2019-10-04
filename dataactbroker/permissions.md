# Permissions

A simple guide covering permissions on the Broker and how they get set on each environment. 

- For more details from an OMB Max permission administration standpoint, see (requires OMB Max login): https://community.max.gov/pages/viewpage.action?spaceKey=TREASExternal&title=DATA+Act+Broker+Registration
- For more details from a code implementation level, please check out the code in [permissions.py](./permissions.py).

## Permission Levels

As listed out in [lookups.py](../dataactcore/models/lookups.py), there are five permission levels.
Each permission level must be granted via MAX groups which only apply to one agency at a time.
Users may be able to have permissions for multiple agencies which is only determined by the MAX groups
they are associated with.

"Permission Levels" can be looked at as _**Roles**_, but are only dealt with in context of a _MAX group_ and _Agency_, as described below.

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

### MAX Groups

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

## Detailed Access Matrix
_Below lists the actions in Broker that a user must be authorized to perform. Authorization is governed by way of granting or denying the action to a **Permission Level (aka Role)** within the context of a MAX Group and Agency._

_The actions below are bucketed by Permission Level._
- _Unless stated that a Permission Level inherits granted or denied actions from another, the Permission Level is **DENIED** all other actions listed below._
- _Given Permission Levels (aka Roles) are assigned on a per-User and per-Agency basis (where the MAX Group defines the Agency), the actions granted or denied below will be done so in the context of a single Agency._

### **`Reader (R)`**
1. Read of everything

### **`Writer (W)`**: 
1. Inherits all permissions of `Reader`
2. Create DABS submission
3. Upload DABS submission files
4. Validate DABS files A, B, C compliance
5. Validate DABS cross-file compliance
6. Generate DABS D1, D2, E, F files
7. Replace DABS submission files
8. Delete DABS submission
9. Update DABS submission comments

### **`Submitter (S)`**
1. Inherits all permissions of `Writer`
2. Certify submission

### **`Edit-FABS (E)`**
1. Inherits all permissions of `Reader`
2. Create FABS submission
3. Uplaod FABS submission file
4. Replace FABS submission file
5. Delete FABS submission file

### **`FABS (F)`**
1. Inherits all permissions of `Edit-FABS`
2. Publish FABS submission data
