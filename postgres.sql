
/*
geoname_id,locale_code,continent_code,continent_name  ,country_iso_code ,country_name                          ,subdivision_1_iso_code ,subdivision_1_name  ,subdivision_2_iso_code ,subdivision_2_name  ,city_name        ,metro_code ,time_zone
88319      ,en        ,AF            ,Africa          ,LY               ,Libya                                 ,BA                     ,"Sha'biyat Banghazi",                       ,                    ,Benghazi		  ,           ,Africa/Tripoli
90552     ,en         ,AS            ,Asia            ,IQ               ,Iraq                                  ,AN                     ,Anbar               ,                       ,                    ,Sulaymaniyah	  ,       	  ,Asia/Baghdad
105072    ,en         ,AS            ,Asia            ,SA               ,"Saudi Arabia"                        ,14                     ,'Asir               ,                       ,                    ,"Khamis Mushait",			  ,Asia/Riyadh
3513173   ,en         ,NA            ,"North America" ,BQ               ,"Bonaire, Sint Eustatius, and Saba"   ,SA                     ,Saba                ,                       ,                    ,"The Bottom"    ,           ,America/Kralendijk
*/

/*			-= Locations =-		*/
/*			Tables				*/
DROP TABLE IF EXISTS public.geoname_location;

CREATE TABLE public.geoname_location
(
	geoname_id			INT4 PRIMARY KEY,
	continent_code		CHAR(2) NOT NULL,
	country_iso_code	CHAR(2) NOT NULL,
	subdivision1_iso_code	VARCHAR(8) NULL,
	subdivision2_iso_code	VARCHAR(8) NULL,
	city_name			VARCHAR(256) NULL
);


DROP TABLE IF EXISTS public.country;

CREATE TABLE public.country
(
	iso2_code	CHAR(2) PRIMARY KEY,
	name		VARCHAR(128) NOT NULL,
	locale		CHAR(2) DEFAULT 'en' NOT NULL
);


DROP TABLE IF EXISTS public.subdivision_1 CASCADE; --drops view also

CREATE TABLE public.subdivision_1
(
	iso2_country_code	CHAR(2) NOT NULL,
	iso_code			VARCHAR(8) NOT NULL,
	name				VARCHAR(128) NOT NULL,
	locale				CHAR(2) DEFAULT 'en' NOT NULL,
	
	PRIMARY KEY(iso2_country_code,iso_code)
);

DROP TABLE IF EXISTS public.subdivision_2 CASCADE;

CREATE TABLE public.subdivision_2
(
	iso2_country_code		CHAR(2) NOT NULL,
	subdivision1_iso_code	VARCHAR(8) NOT NULL,
	
	iso_code	VARCHAR(8) NOT NULL,
	name 		VARCHAR(128) NOT NULL,
	locale 		CHAR(2) DEFAULT 'en' NOT NULL,
	
	PRIMARY KEY(iso2_country_code,subdivision1_iso_code,iso_code)
);

/*	network
network    ,geoname_id ,registered_country_geoname_id ,represented_country_geoname_id ,is_anonymous_proxy ,is_satellite_provider ,postal_code ,latitude ,longitude ,accuracy_radius
1.0.0.0/24 ,2077456    ,2077456                       ,                               ,0                  ,0                     ,            ,-33.4940 ,143.2104  ,1000
1.0.1.0/24 ,1810821    ,1814991                       ,                               ,0                  ,0                     ,            ,26.0614  ,119.3061  ,50
1.0.2.0/23 ,1810821    ,1814991                       ,                               ,0                  ,0                     ,            ,26.0614  ,119.3061  ,50
*/

/*			-= Network =-		*/
DROP TABLE IF EXISTS public.geoip CASCADE;

CREATE TABLE public.geoip
(
	network		inet PRIMARY KEY,
	geoname_id	INT4 NOT NULL,
	postal_code	VARCHAR(16) NULL
);


/*		Views			*/

DROP VIEW IF EXISTS public.GeonameLocation;

CREATE VIEW GeonameLocation AS		
SELECT	loc.geoname_id, 
		loc.continent_code, 
		loc.country_iso_code, 
		cc.name, 
		sub1.iso_code as subdivision1_code, 
		sub1.name as subdivision1,
		sub2.iso_code as subdivision2_code, 
		sub2.name as subdivision2, 
		loc.city_name 
FROM geoname_location loc 
 INNER JOIN country cc
  ON loc.country_iso_code = cc.iso2_code
 LEFT OUTER JOIN subdivision_1 sub1
  ON loc.country_iso_code = sub1.iso2_country_code
  AND loc.subdivision1_iso_code = sub1.iso_code
 LEFT OUTER JOIN subdivision_2 sub2
  ON sub1.iso2_country_code = sub2.iso2_country_code
	AND sub1.iso_code = sub2.subdivision1_iso_code
	AND loc.subdivision2_iso_code = sub2.iso_code;

DROP VIEW IF EXISTS public.GeoipNetwork;

CREATE VIEW public.GeoipNetwork AS
SELECT ip.network,ip.postal_code, loc.* 
FROM geoip ip
 INNER JOIN GeonameLocation loc
  ON ip.geoname_id = loc.geoname_id;

/*		Functions		*/
CREATE OR REPLACE
FUNCTION add_geoip( p_network		inet,
					p_geoname_id	INT4,
					p_postal_code	VARCHAR(16) )
RETURNS INT4 AS $$
DECLARE
	p_current inet;
BEGIN

	-- check if already exists
	/*SELECT	network INTO p_current 
	FROM	geoip 
	WHERE	network = p_network;
	IF p_current IS NOT NULL THEN
		RETURN 0;
	END IF;*/
	
	-- Ensure geoname_id exists
	IF NOT EXISTS (SELECT geoname_id FROM geoname_location WHERE geoname_id=p_geoname_id) THEN
		RAISE EXCEPTION 'Geoname Id Not Found';
	END IF;

	INSERT INTO geoip (network,geoname_id,postal_code) 
	VALUES (p_network,p_geoname_id,p_postal_code);

RETURN 0;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE
FUNCTION add_country( p_iso2_country_code CHAR(2), p_country_name VARCHAR(128) )
RETURNS INT4 AS $$
DECLARE
	p_iso2 CHAR(2);
BEGIN

	SELECT iso2_code INTO p_iso2 FROM country WHERE iso2_code = p_iso2_country_code;
	IF p_iso2 IS NOT NULL THEN
		RETURN 0;
	END IF;
	
	INSERT INTO country (iso2_code,name) 
	VALUES (p_iso2_country_code,p_country_name);

RETURN 0;
END
$$ LANGUAGE plpgsql;

/*	-- add_subdivision1		*/
CREATE OR REPLACE
FUNCTION add_subdivision1( p_iso2_country_code CHAR(2), p_iso_code VARCHAR(8), p_name VARCHAR(128)  )
RETURNS INT4 AS $$
DECLARE
	p_iso_current VARCHAR(8);
BEGIN
	
	SELECT	iso_code INTO p_iso_current 
	FROM	subdivision_1 
	WHERE	iso2_country_code = p_iso2_country_code 
	AND		iso_code = p_iso_code;
	IF p_iso_current IS NOT NULL THEN
		RETURN 0;
	END IF;
	
	INSERT INTO subdivision_1 (iso2_country_code,iso_code,name) 
	VALUES (p_iso2_country_code,p_iso_code,p_name);

RETURN 0;
END
$$ LANGUAGE plpgsql;


/*	-- add_subdivision2		*/
CREATE OR REPLACE
FUNCTION add_subdivision2( p_iso2_country_code CHAR(2), p_subdivision1_iso_code	VARCHAR(8),
						   p_iso_code VARCHAR(8), p_name VARCHAR(128) )
RETURNS INT4 AS $$
DECLARE
	p_iso_current VARCHAR(8);
BEGIN
	
	SELECT	iso_code INTO p_iso_current 
	FROM	subdivision_2 
	WHERE	iso2_country_code = p_iso2_country_code
	AND		subdivision1_iso_code = p_subdivision1_iso_code
	AND		iso_code = p_iso_code;
	IF p_iso_current IS NOT NULL THEN
		RETURN 0;
	END IF;
	
	INSERT INTO subdivision_2 (iso2_country_code,subdivision1_iso_code,iso_code,name) 
	VALUES (p_iso2_country_code,p_subdivision1_iso_code,p_iso_code,p_name);

RETURN 0;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE
FUNCTION add_geoname_location( p_geoname_id INT4,
								p_continent_code CHAR(2),
								p_city_name VARCHAR(256),
								p_iso2_country_code CHAR(2), p_country_name VARCHAR(128),
								p_subdivision_1_iso_code VARCHAR(8), 
								p_subdivision_1_name VARCHAR(128),
								p_subdivision_2_iso_code VARCHAR(8), 
								p_subdivision_2_name VARCHAR(128) )
RETURNS INT4 AS $$
DECLARE
	p_current_id INT4;
BEGIN
	-- Add the country
	PERFORM add_country(p_iso2_country_code,p_country_name);
	
	IF p_subdivision_1_iso_code IS NOT NULL THEN
		PERFORM add_subdivision1(p_iso2_country_code,p_subdivision_1_iso_code,p_subdivision_1_name);
	END IF;
	
	IF p_subdivision_2_iso_code IS NOT NULL THEN
		PERFORM add_subdivision2(p_iso2_country_code,p_subdivision_1_iso_code, p_subdivision_2_iso_code,p_subdivision_2_name);
	END IF;
	
	SELECT	geoname_id INTO p_current_id 
	FROM	geoname_location 
	WHERE	geoname_id = p_geoname_id;
	IF p_current_id IS NOT NULL THEN
		RETURN 0; -- value already exists.
	END IF;
	
	INSERT INTO geoname_location (
		geoname_id,
		continent_code,
		country_iso_code,
		subdivision1_iso_code,
		subdivision2_iso_code,
		city_name ) 
	VALUES (
		p_geoname_id, 
		p_continent_code,
		p_iso2_country_code,
		p_subdivision_1_iso_code,
		p_subdivision_2_iso_code,
		p_city_name );

RETURN 0;
END
$$ LANGUAGE plpgsql;


