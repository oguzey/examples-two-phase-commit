# sudo -u postgres psql
CREATE DATABASE data_fly_booking;

# psql -h localhost data_fly_booking test_user
CREATE SEQUENCE fly_ids;

CREATE TABLE fly_booking (
	booking_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('fly_ids'),
	client_name VARCHAR(64),
	fly_number VARCHAR(64),
	"from" VARCHAR(16),
	"to" VARCHAR(16),
	date DATE);

# sudo -u postgres psql
CREATE DATABASE data_hotel_booking;

# psql -h localhost data_hotel_booking test_user
CREATE SEQUENCE hotel_ids;

CREATE TABLE hotel_booking (
	booking_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('hotel_ids'),
	client_name VARCHAR(64),
	hotel_name VARCHAR(64),
	arrival DATE,
	departure DATE);