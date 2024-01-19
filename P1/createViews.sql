-- View_User_Information
CREATE VIEW View_User_Information AS 
SELECT u.user_id, u.first_name, u.last_name, u.year_of_birth, u.month_of_birth, u.day_of_birth, u.gender, acc.city_name AS current_city, acc.state_name AS current_state, acc.country_name AS current_country, ah.city_name AS hometown_city, ah.state_name AS hometown_state, ah.country_name AS hometown_country, p.institution, e.program_year, p.concentration, p.degree
FROM Users u 
LEFT JOIN User_Current_Cities cc ON u.user_id = cc.user_id
LEFT JOIN User_Hometown_Cities h ON u.user_id = h.user_id 
LEFT JOIN Cities acc ON cc.current_city_id = acc.city_id
LEFT JOIN Cities ah ON h.hometown_city_id = ah.city_id
LEFT JOIN Education e ON u.user_id = e.user_id
LEFT JOIN Programs p ON e.program_id = p.program_id
WHERE u.year_of_birth IS NOT NULL AND u.month_of_birth IS NOT NULL AND u.day_of_birth IS NOT NULL AND u.gender IS NOT NULL;
-- View_Are_Friends
CREATE VIEW View_Are_Friends AS 
SELECT user1_id, user2_id
FROM Friends;
-- View_Photo_Information
CREATE VIEW View_Photo_Information AS 
SELECT a.album_id, a.album_owner_id, a.cover_photo_id, a.album_name, a.album_created_time, a.album_modified_time, a.album_link, a.album_visibility, p.photo_id, p.photo_caption, p.photo_created_time, p.photo_modified_time, p.photo_link
FROM Albums a 
JOIN Photos p ON a.album_id = p.album_id;
-- View_Event_Information
CREATE VIEW View_Event_Information AS
SELECT e.event_id, e.event_creator_id, e.event_name, e.event_tagline, e.event_description, e.event_host, e.event_type, e.event_subtype, e.event_address, c.city_name AS event_city, c.state_name AS event_state, c.country_name AS event_country, e.event_start_time, e.event_end_time
FROM User_Events e
LEFT JOIN Cities c ON e.event_city_id = c.city_id;
-- View_Tag_Information
CREATE VIEW View_Tag_Information AS
SELECT t.tag_photo_id, t.tag_subject_id, t.tag_created_time, t.tag_x, t.tag_y
FROM Tags t;
