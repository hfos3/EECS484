INSERT INTO Users(user_id,first_name,last_name,year_of_birth,month_of_birth,day_of_birth,gender)
SELECT DISTINCT user_id,first_name,last_name,year_of_birth,month_of_birth,day_of_birth,gender
FROM project1.Public_User_Information;


INSERT INTO Cities(city_name,state_name,country_name)
SELECT DISTINCT current_city,current_state,current_country
FROM project1.Public_User_Information;

INSERT INTO Cities(city_name,state_name,country_name)
SELECT DISTINCT hometown_city, hometown_state, hometown_country
FROM project1.Public_User_Information
WHERE hometown_city NOT IN (SELECT city_name FROM Cities) OR
hometown_state NOT IN (SELECT state_name FROM Cities) OR 
hometown_country NOT IN (SELECT country_name FROM Cities);

INSERT INTO Cities(city_name,state_name,country_name)
SELECT DISTINCT event_city,event_state, event_country
FROM project1.Public_Event_Information
WHERE event_city NOT IN (SELECT city_name FROM Cities) OR
event_state NOT IN (SELECT state_name FROM Cities) OR 
event_country NOT IN (SELECT country_name FROM Cities);

INSERT INTO User_Current_Cities(user_id,current_city_id)
SELECT DISTINCT project1.Public_User_Information.user_id, Cities.city_id
FROM Cities
JOIN project1.Public_User_Information ON project1.Public_User_Information.current_city = Cities.city_name AND project1.Public_User_Information.current_state = Cities.state_name AND project1.Public_User_Information.current_country = Cities.country_name;

INSERT INTO User_Hometown_Cities(user_id,hometown_city_id)
SELECT DISTINCT user_id, city_id
FROM Cities
JOIN project1.Public_User_Information ON project1.Public_User_Information.hometown_city = Cities.city_name AND project1.Public_User_Information.hometown_state = Cities.state_name AND project1.Public_User_Information.hometown_country = Cities.country_name;

INSERT INTO Programs(institution,concentration,degree)
SELECT DISTINCT institution_name,program_concentration,program_degree
FROM project1.Public_User_Information
WHERE institution_name IS NOT NULL OR program_concentration IS NOT NULL OR program_degree IS NOT NULL OR program_year IS NOT NULL;

INSERT INTO Education(user_id,program_id,program_year)
SELECT user_id,program_id,program_year
FROM Programs
JOIN project1.Public_User_Information ON project1.Public_User_Information.institution_name = Programs.institution AND project1.Public_User_Information.program_degree = Programs.degree AND project1.Public_User_Information.program_concentration = Programs.concentration;

SET AUTOCOMMIT OFF;

INSERT INTO Albums(album_id,album_owner_id,album_name,album_created_time,album_modified_time,album_link,album_visibility,cover_photo_id)
SELECT DISTINCT album_id,owner_id,album_name,album_created_time,album_modified_time,album_link,album_visibility,cover_photo_id
FROM project1.Public_Photo_Information;


INSERT INTO Photos(photo_id,album_id,photo_caption,photo_created_time,photo_modified_time,photo_link)
SELECT DISTINCT photo_id,album_id,photo_caption,photo_created_time,photo_modified_time,photo_link
FROM project1.Public_Photo_Information;

COMMIT;
SET AUTOCOMMIT ON;

INSERT INTO User_Events(event_id,event_creator_id,event_name,event_tagline,event_description,event_host,event_type,event_subtype,event_address,event_city_id,event_start_time,event_end_time)
SELECT event_id,event_creator_id,event_name,event_tagline,event_description,event_host,event_type,event_subtype,event_address,city_id,event_start_time,event_end_time
FROM Cities
JOIN project1.Public_Event_Information ON project1.Public_Event_Information.event_city = Cities.city_name;


INSERT INTO Friends(user1_id,user2_id)
SELECT DISTINCT user1_id, user2_id
FROM project1.Public_Are_Friends;

INSERT INTO Tags(tag_photo_id, tag_subject_id, tag_created_time, tag_x, tag_y)
SELECT DISTINCT photo_id, tag_subject_id, tag_created_time, tag_x_coordinate, tag_y_coordinate
FROM project1.Public_Tag_Information;