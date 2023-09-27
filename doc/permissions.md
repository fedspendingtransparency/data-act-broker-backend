# Permissions

A simple guide covering permissions on the Broker and how they get set on each environment. 

- For more details from a CAIA permission administration standpoint, see the [IIQ User Guide](https://caia.treasury.gov/developers/IIQUserGuide/) (requires CAIA login):
- For more details from a code implementation level, please check out the code in [permissions.py](../dataactbroker/permissions.py).

## Broker Permission Levels

As listed out in [lookups.py](../dataactcore/models/lookups.py), there are five permission levels, all of which
determine how a user may interact with the system in the context of their associated agencies.

Broker users are assigned permission levels via [CAIA roles managed by the CAIA IIQ service](#caia). Users may be able
to have permissions levels for multiple agencies.

"Permission Levels" can be looked at as overall _**Roles**_, but are only dealt within the context of an associated _Agency_.
Additionally, these permission levels pertain only to and explain the behavior of the Broker application. They
exist solely within the Broker database, independent from the system that manages the agency users and its roles.

#### DABS (Data Act Broker Submissions)

1. **Reader (R)**: May read all aspects of submissions associated with the agency (both DABS and FABS).
2. **Writer (W)**: May go through the submission process up to certifying (includes uploads, 
advancing in the submission, and deleting submissions).
3. **Submitter (S)**: May certify submissions for the agency.

#### FABS (Financial Assistance Broker Submissions)

1. **Reader (R)**: May read all aspects of submissions associated with the agency (both DABS and FABS).
2. **Edit-FABS (E)**: May upload files and delete submissions.
3. **FABS (F)**: May publish FABS submissions.

**Note**: For each group (DABS/FABS), each additional permission level builds upon the previous level
(ex. DABS Writer can also read DABS Submissions, FABS can also upload FABS files).

#### Administrators

Specific users may be given administrator access which includes all the access listed above.

## CAIA

Each time a user logs into the Broker, they go through the Department of Treasury's CAIA (Common Approach to Identity Assurance),
a system allowing various sources of government users to authenticate in one place.

After authenticating, the Broker reads the CAIA roles the logged-in user is assigned and resets their Broker permission levels
based on the agency code and permission level included in each of the CAIA role names.

### Navigating CAIA IIQ

The management system of the CAIA users and their associated roles is known as the [CAIA IIQ service](https://iiq.fiscal.treasury.gov/) (requires CAIA login).

Each agency has assigned at least one **Agency Administrator** (also referred to as the **AppOwner**) that is responsible for managing the users and their associated roles
within the agency. Each agency admin should be aware of the common workflows below.

Each workflow will be referencing the [IIQ User Guide](https://caia.treasury.gov/developers/IIQUserGuide/) as it includes
step-by-step instructions with screenshots.

#### Agency Users: Requesting Broker Permission Roles within an Agency

Agency Administrators are unable to approve their own IIQ requests; **therefore, the user seeking access will need to self-request.**

The user can follow the instructions in the [IIQ User Guide](https://caia.treasury.gov/developers/IIQUserGuide/) ("*Self Request Access*").

To determine the proper CAIA role to add, refer to the [Roles Names](#role-names) section below.

#### Agency Admins: Approving/Denying Requests

Once the agency user has requested the proper CAIA role, the request needs to be approved or denied by the Agency Admin.
The Agency Admin CAIA role includes the unique ability to see, approve, and deny these requests. 

The agency admin can follow the instructions in the [IIQ User Guide](https://caia.treasury.gov/developers/IIQUserGuide/) ("*Manage User Access*" -> "*Approving/Denying User Access Requests*").

Once approved, the agency user will be assigned the role shortly (at least several minutes).

#### Removing User Access

If a user needs to have a certain role removed, either the agency user or the agency admin can revoke the permission without needing approval. 

The agency admin can follow the instructions in the [IIQ User Guide](https://caia.treasury.gov/developers/IIQUserGuide/) ("*Manage User Access*" -> "*Removing User Access*").

#### Identifying Users Roles

At the moment, the only way to determine the currently assigned CAIA roles of a certain user is by following the workflow to
remove their access (see above), note which roles are available to be removed, and not finally requesting to have any removed.

#### Adding/Removing Agency Admins

To assign a user to be an agency admin, the user must request to be assigned the role.
Agency admins are unable to approve/deny their own requests and so **it's required for the user to make the request.**

The user can follow the guide above to request the agency-specific Agency Admin role and
the existing agency admin can follow the other guide above to approve/deny the request, making the user an agency admin.

If an agency admin plans to transfer or remove their agency admin status, it is neccessary to ensure that at least one
agency admin is assigned to that agency and the agency admin CAIA role is removed from the previous owner (following the
removing user access guide above).

#### Troubleshooting
* If a user has left their agency admin role without removing it or if an agency is unable to assign an agency admin,
contact the [ServiceDesk](mailto:usaspending.help@fiscal.treasury.gov).

### Role Names

Below are the CAIA role name templates that the Broker uses to determine which agency permission levels to assign to each user.

#### Components

- `CGAC Code`: The 3-digit code representing the toptier agency this permission is associated with.
- `FREC Code`: The 4-digit FR entity code representing the toptier agency this permission is associated with.
- `Permission Level`: One of the levels [listed above](#broker-permission-levels) represented by their letter code

#### Formats

Broker Permission Levels/Roles (within an agency):
- CGAC agencies: **Data_Act_Broker-CGAC-`CGAC Code`-`Permission Level`**
- FREC agencies: **Data_Act_Broker-FREC-`FREC Code`-`Permission Level`**

**Note: FREC permissions will give read access to the CGAC agency as well.**

Agency Administrators Roles:
- CGAC agencies: **AppOwner-Data_Act_Broker-`CGAC Code`**
- FREC agencies: **AppOwner-Data_Act_Broker-`FREC Code`**

For more details, check out [account_handler.py](../dataactbroker/handlers/account_handler.py).

## Detailed Access Matrix
_Below lists the actions in Broker that a user must be authorized to perform. Authorization is governed by way of granting or denying the action to a **Permission Level (aka Role)** within the context of an Agency._

_The actions below are bucketed by Permission Level._
- _Unless stated that a Permission Level inherits granted or denied actions from another, the Permission Level is **DENIED** all other actions listed below._
- _Given Permission Levels (aka Roles) are assigned on a per-User and per-Agency basis (where the CAIA role name includes the Agency), the actions granted or denied below will be done so in the context of a single Agency._

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
3. Upload FABS submission file
4. Replace FABS submission file
5. Delete FABS submission file

### **`FABS (F)`**
1. Inherits all permissions of `Edit-FABS`
2. Publish FABS submission data
