DROP TABLE IF EXISTS Votes;
DROP TABLE IF EXISTS Actions;
DROP TABLE IF EXISTS Members;
DROP TABLE IF EXISTS Projects;
DROP TABLE IF EXISTS AllId;
DROP ROLE IF EXISTS app;



create table Projects  (
    id int unique ,
    authorityid int,
    PRIMARY KEY (id)
);

create table Members  (
    member int unique,
    password text not null,
    timestamp int,
    is_leader int,
    PRIMARY KEY(member));

create table Actions  (
    id int unique not null,
    project_id int,
    member_id int,
    type text not null,
    timestamp int,
    votes_for int default 0,
    votes_against int default 0,
    PRIMARY KEY (id),

    CONSTRAINT action_projectid_fkey
    FOREIGN KEY (project_id)
    REFERENCES Projects(id),

    CONSTRAINT action_memberid_fkey
        FOREIGN KEY (member_id)
        REFERENCES Members (member)
 );

create table Votes  (
    action_id int ,
    member_id int,
    voted_for int default 0,
    voted_against int default 0,

   CONSTRAINT votes_memberid_fkey
        FOREIGN KEY (member_id)
        REFERENCES Members (member),

   CONSTRAINT votes_actionid_fkey
	FOREIGN KEY(action_id)
	REFERENCES Actions(id)
);



create table AllId (
        id int unique
);

CREATE USER app with password 'qwerty' ;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO app;


