

CREATE TABLE Games_2023 (
    game_id         INT unsigned zerofill   NOT NULL AUTO_INCREMENT,
    week            TINYINT unsigned    NOT NULL,
    kickoff_time    DATETIME            NOT NULL,
    away_team_id    CHAR(3)             NOT NULL,
    home_team_id    CHAR(3)             NOT NULL,
    home_team_line  SMALLINT        DEFAULT null,
    away_team_score INT unsigned    DEFAULT null,
    home_team_score INT unsigned    DEFAULT null,
    away_team_ats   INT DEFAULT null,
    home_team_ats   INT DEFAULT null,

    PRIMARY KEY (game_id),

    FOREIGN KEY (away_team_id)
        REFERENCES Teams(team_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    FOREIGN KEY (home_team_id)
        REFERENCES Teams(team_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT

) ENGINE=InnoDB;


CREATE TABLE Picks_2023 (
    pick_id     INT unsigned zerofill   NOT NULL AUTO_INCREMENT,
    player_id   INT unsigned zerofill   NOT NULL,
    week        TINYINT unsigned        NOT NULL,
    pick        CHAR(3)                 DEFAULT 'NOP',
    pick_ats    INT                     DEFAULT null,
    submit_time     DATETIME            DEFAULT null,
    lock_in_time    DATETIME            DEFAULT null,

    PRIMARY KEY (pick_id),

    FOREIGN KEY (player_id)
        REFERENCES Players(player_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    FOREIGN KEY (pick)
        REFERENCES Teams(team_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT

) ENGINE=InnoDB;


CREATE TABLE Standings_2023 (
    player_id       INT unsigned zerofill   NOT NULL,
    wins            TINYINT unsigned        NOT NULL DEFAULT '0',
    losses          TINYINT unsigned        NOT NULL DEFAULT '0',
    win_percentage  FLOAT(4,3)              NOT NULL DEFAULT '0',
    ats_points      INT                     NOT NULL DEFAULT '0',

    PRIMARY KEY (player_id),

    FOREIGN KEY (player_id)
        REFERENCES Players(player_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT

) ENGINE=InnoDB;

CREATE TABLE Standings_Message_2023 (
   message_id     int unsigned zerofill   NOT NULL auto_increment,
   week           tinyint unsigned        NOT NULL,
   message        text                    NOT NULL,

   PRIMARY KEY (message_id)

) ENGINE=InnoDB;

ALTER TABLE Players
ADD COLUMN 2023_registration tinyint unsigned   DEFAULT NULL;

ALTER TABLE Players
ADD COLUMN 2023_paid tinyint unsigned   DEFAULT NULL;

