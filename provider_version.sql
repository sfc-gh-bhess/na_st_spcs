USE ROLE naspcs_role;

-- Build Docker image and push to repo via make 
-- Upload files to Stage
ALTER APPLICATION PACKAGE na_st_spcs_pkg ADD VERSION v2 USING @spcs_app.napp.app_stage/na_st_spcs/v2;

-- for subsequent updates to version
ALTER APPLICATION PACKAGE na_st_spcs_pkg ADD PATCH FOR VERSION v2 USING @spcs_app.napp.app_stage/na_st_spcs/v2
