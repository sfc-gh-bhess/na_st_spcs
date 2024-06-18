# Example Native App with Snowpark Conatiner Services
This is a simple Native App that uses Snowpark Container
Services to deploy a frontend application. It queries the 
TPC-H 100 data set and returns the top sales clerks. The 
frontend provides date pickers to restrict the range of the sales
data and a slider to determine how many top clerks to display.
The data is presented in a table sorted by highest seller
to lowest. This example uses Streamlit for the frontend.

## Setup
There are 2 parts to set up, the Provider and the Consumer.

This example expects that both Provider and Consumer have been
set up with the prerequisite steps to enable for Snowpark 
Container Services, specifically:
```sql
USE ROLE ACCOUNTADMIN;
CREATE SECURITY INTEGRATION IF NOT EXISTS snowservices_ingress_oauth
  TYPE=oauth
  OAUTH_CLIENT=snowservices_ingress
  ENABLED=true;
```

### Provider Setup
For the Provider, we need to set up only a few things:
* A STAGE to hold the files for the Native App
* An IMAGE REPOSITORY to hold the image for the service image
* An APPLICATION PACKAGE that defines the Native App

As `ACCOUNTADMIN` run the commands in `provider_setup.sql`.

To enable the setup, we will use some templated files. There 
is a script to generate the files from the templated files. 
You will need the following as inputs:
* The full name of the image repository. You can get this by running 
   `SHOW IMAGE REPOSITORIES IN SCHEMA spcs_app.napp;`, and getting the `repository_url`.

To create the files, run:

```bash
bash ./config_napp.sh
```

This created a `Makefile` with the necessary repository filled in. Feel free to look
at the Makefile, but you can also just run:

```bash
make all
```

This will create the 1 container image and push it to the IMAGE REPOSITORY.

Next, you need to upload the files in the `na_st_spcs/v2` directory into the stage 
`SPCS_APP.NAPP.APP_STAGE` in the folder `na_st_spcs/v2`.

To create the VERSION for the APPLICATION PACKAGE, run the following commands
(they are also in `provider_version.sql`):

```sql
USE ROLE naspcs_role;
-- for the first version of a VERSION
ALTER APPLICATION PACKAGE na_st_spcs_pkg ADD VERSION v2 USING @spcs_app.napp.app_stage/na_st_spcs/v2;
```

If you need to iterate, you can create a new PATCH for the version by running this
instead:

```sql
USE ROLE naspcs_role;
-- for subsequent updates to version
ALTER APPLICATION PACKAGE na_st_spcs_pkg ADD PATCH FOR VERSION v2 USING @spcs_app.napp.app_stage/na_st_spcs/v2;
```

### Testing on the Provider Side

#### Setup for Testing on the Provider Side
We can test our Native App on the Provider by mimicking what it would look like on the 
Consumer side (a benefit/feature of the Snowflake Native App Framework).

To do this, run the commands in `consumer_setup.sql`. This will create the role, 
virtual warehouse for install, database, schema,  VIEW of the TPC-H data, and 
permissions necessary to configure the Native App. The ROLE you will use for this is `NAC`.

#### Testing on the Provider Side
First, let's install the Native App.

Run the commands in `provider_test.sql`.

Next we need to configure the Native App. We can do this via Snowsight by
visiting the Apps tab and clicking on our Native App `NA_ST_SPCS_APP`.
* Click the "Grant" button to grant the necessary privileges
* Click the "Review" button to open the dialog to create the
  necessary `EXTERNAL ACCESS INTEGRATION`. Review the dialog and
  click "Connect".

At this point, you should now see an "Activate" button in the top right.
Click it to activate the app.

Once it has successfully activated, the "Activate" button will be replaced
with a "Launch app" button. Click the "Launch app" button to open the
containerized Streamlit app in a new tab.

At this point, you can also grant access to the ingress endpoint by granting
the APPLICATION ROLE `app_user` to a normal user role. Users with that role can
then visit the URL.

If you need to get the URL via SQL, you can call a stored procedure 
in the Native App, `app_public.app_url()`.

##### Cleanup
To clean up the Native App test install, you can just `DROP` it:

```sql
DROP APPLICATION na_st_spcs_app CASCADE;
```
The `CASCADE` will also drop the `WAREHOUSE` and `COMPUTE POOL` that the
Application created, along with the `EXTERNAL ACCESS INTEGRATION` that 
the Application prompted the Consumer to create.

### Testing a failed upgrade
The objects in the `na_st_spcs/v2_bad` directory contain code that will fail
on version_initialization. In the `config.upgrade_service_st_spcs()` it throws
an explicit exception after altering the service.

You need to upload the files in the `na_st_spcs/v2_bad` directory into the stage 
`SPCS_APP.NAPP.APP_STAGE` in the folder `na_st_spcs/v2_bad`.

To create the VERSION for the APPLICATION PACKAGE, run the following commands
```sql
USE ROLE naspcs_role;
-- for subsequent updates to version
ALTER APPLICATION PACKAGE na_st_spcs_pkg ADD PATCH FOR VERSION v2 USING @spcs_app.napp.app_stage/na_st_spcs/v2_bad;
```

Note the patch number.

To attempt to upgrade the application, run the following:
```sql
USE ROLE nac;
ALTER APPLICATION na_st_spcs_app UPGRADE USING VERSION v2 PATCH <PATCH_NUMBER>;
```

This should fail. You can see in the event table a series of log messages that
begin with `!BAD!`, followed by the log messages from the version initialization
from the previous successful version.


### Publishing/Sharing your Native App
You Native App is now ready on the Provider Side. You can make the Native App available
for installation in other Snowflake Accounts by setting a default PATCH and Sharing the App
in the Snowsight UI.

Navigate to the "Apps" tab and select "Packages" at the top. Now click on your App Package 
(`NA_ST_SPCS_PKG`). From here you can click on "Set release default" and choose the latest patch
(the largest number) for version `v2`. 

Next, click "Share app package". This will take you to the Provider Studio. Give the listing
a title, choose "Only Specified Consumers", and click "Next". For "What's in the listing?", 
select the App Package (`NA_ST_SPCS_PKG`). Add a brief description. Lastly, add the Consumer account
identifier to the "Add consumer accounts". Then click "Publish".

### Using the Native App on the Consumer Side

#### Setup for Testing on the Consumer Side
We're ready to import our Native App in the Consumer account.

To do the setup, run the commands in `consumer_setup.sql`. This will create the role and
virtual warehouse for the Native App. The ROLE you will use for this is `NAC`.

#### Using the Native App on the Consumer
To get the Native app, navigate to the "Apps" sidebar. You should see the app at the top under
"Recently Shared with You". Click the "Get" button. Select a Warehouse to use for installation.
Under "Application name", choose the name `NA_ST_SPCS_APP` (You _can_ choose a 
different name, but the scripts use `NA_ST_SPCS_APP`). Click "Get".

Next we need to configure the Native App. We can do this via Snowsight by
visiting the Apps tab and clicking on our Native App `NA_ST_SPCS_APP`.
* Click the "Grant" button to grant the necessary privileges
* Click the "Review" button to open the dialog to create the
  necessary `EXTERNAL ACCESS INTEGRATION`. Review the dialog and
  click "Connect".

At this point, you should now see an "Activate" button in the top right.
Click it to activate the app.

Once it has successfully activated, the "Activate" button will be replaced
with a "Launch app" button. Click the "Launch app" button to open the
containerized Streamlit app in a new tab.

At this point, you can also grant access to the ingress endpoint by granting
the APPLICATION ROLE `app_user` to a normal user role. Users with that role can
then visit the URL.

If you need to get the URL via SQL, you can call a stored procedure 
in the Native App, `app_public.app_url()`.

##### Cleanup
To clean up the Native App, you can just uninstall it from the "Apps" tab.

#### Debugging
I added some debugging Stored Procedures to allow the Consumer to see the status
and logs for the containers and services. These procedures are granted to the `app_admin`
role and are in the `app_public` schema:
* `GET_SERVICE_STATUS()` which takes the same arguments and returns the same information as `SYSTEM$GET_SERVICE_STATUS()`
* `GET_SERVICE_LOGS()` which takes the same arguments and returns the same information as `SYSTEM$GET_SERVICE_LOGS()`

The permissions to debug are managed on the Provider in the 
`NA_ST_SPCS_PKG.SHARED_DATA.FEATURE_FLAGS` table. 
It has a very simple schema:
* `acct` - the Snowflake account to enable. This should be set to the value of `SELECT current_account()` in that account.
* `flags` - a VARIANT object. For debugging, the object should have a field named `debug` which is an 
  array of strings. These strings enable the corresponding stored procedure:
  * `GET_SERVICE_STATUS`
  * `GET_SERVICE_LOGS`

An example of how to enable logging for a particular account (for example, account 
`ABC12345`) to give them all the debugging permissions would be

```sql
INSERT INTO na_st_spcs_pkg.shared_data.feature_flags 
  SELECT parse_json('{"debug": ["GET_SERVICE_STATUS", "GET_SERVICE_LOGS"]}') AS flags, 
         'ABC12345' AS acct;
```

To enable on the Provider account for use while developing on the Provider side, you could run

```sql
INSERT INTO na_st_spcs_pkg.shared_data.feature_flags 
  SELECT parse_json('{"debug": ["GET_SERVICE_STATUS", "GET_SERVICE_LOGS"]}') AS flags,
         current_account() AS acct;
```
