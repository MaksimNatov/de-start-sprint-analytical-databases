-- DDL для нового источника
CREATE TABLE STV202506164__STAGING.group_log(
	group_id int PRIMARY KEY,
	user_id INT,
	user_id_from INT,
	event varchar(100),
	datetime TIMESTAMP 	
);


-- DDL для новых таблиц в DDS
DROP TABLE IF EXISTS STV202506164__DWH.l_user_group_activity;
DROP TABLE IF EXISTS STV202506164__DWH.s_auth_history;
-- Cкрипт создания l_user_group_activity
create table STV202506164__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV202506164__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES STV202506164__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-- Cкрипт наполнения l_user_group_activity
INSERT INTO STV202506164__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
SELECT
DISTINCT 
hash(hu.hk_user_id, hg.hk_group_id),
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV202506164__STAGING.group_log as g
left join STV202506164__DWH.h_users as hu on g.user_id = hu.user_id
left join STV202506164__DWH.h_groups as hg on g.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV202506164__DWH.l_user_group_activity);
--Скрипт создания s_auth_history
create table STV202506164__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV202506164__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(100),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
-- Cкрипт наполнения s_auth_history
INSERT INTO STV202506164__DWH.s_auth_history(hk_l_user_group_activity,user_id_from, event, event_dt, load_dt, load_src)
select 
luga.hk_l_user_group_activity,
gl.user_id_from,
gl.event,
gl.datetime,
now() as load_dt,
's3' as load_src
from STV202506164__STAGING.group_log as gl
left join STV202506164__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV202506164__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV202506164__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;



--CTE
WITH user_group_messages AS (
	SELECT -- считаем кол-во сообщений
		gd.hk_group_id,
		COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
	FROM
		STV202506164__DWH.l_groups_dialogs gd
	JOIN STV202506164__DWH.l_user_message lum ON gd.hk_message_id = lum.hk_message_id
	GROUP BY 1
	ORDER BY 1
),
old_gr AS (
	SELECT -- фильтруем 10 старых групп
		hk_group_id,
		registration_dt
	FROM STV202506164__DWH.h_groups
	ORDER BY 2
	LIMIT 10
),
user_group_log AS (
    SELECT -- считаем кол-во юзеров
    	ga.hk_group_id,
    	COUNT( DISTINCT ga.hk_user_id) AS cnt_added_users
    FROM STV202506164__DWH.l_user_group_activity ga
    JOIN STV202506164__DWH.s_auth_history ah ON ah.hk_l_user_group_activity = ga.hk_l_user_group_activity
    JOIN old_gr ON old_gr.hk_group_id = ga.hk_group_id
    WHERE ah.event = 'add'
    GROUP BY 1
)
SELECT 
	gl.hk_group_id,
	gl.cnt_added_users,
	gm.cnt_users_in_group_with_messages,
	ROUND(gm.cnt_users_in_group_with_messages/gl.cnt_added_users * 100, 2) AS group_conversion
FROM user_group_log gl
JOIN user_group_messages gm ON gl.hk_group_id = gm.hk_group_id 
