CREATE TABLE Games_{YEAR} (
    game_id         INT UNSIGNED        NOT NULL AUTO_INCREMENT,
    week            TINYINT UNSIGNED    NOT NULL,
    kickoff_time    DATETIME            NOT NULL,
    away_team_id    CHAR(3)             NOT NULL,
    home_team_id    CHAR(3)             NOT NULL,
    home_team_line  SMALLINT            DEFAULT NULL,
    away_team_score INT UNSIGNED        DEFAULT NULL,
    home_team_score INT UNSIGNED        DEFAULT NULL,
    away_team_ats   INT                 DEFAULT NULL,
    home_team_ats   INT                 DEFAULT NULL,

    PRIMARY KEY (game_id),

    FOREIGN KEY (away_team_id)
        REFERENCES Teams(team_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    FOREIGN KEY (home_team_id)
        REFERENCES Teams(team_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE Picks_{YEAR} (
    pick_id         INT UNSIGNED        NOT NULL AUTO_INCREMENT,
    player_id       INT UNSIGNED        NOT NULL,
    week            TINYINT UNSIGNED    NOT NULL,
    pick            CHAR(3)             DEFAULT 'NOP',
    pick_ats        INT                 DEFAULT NULL,
    submit_time     DATETIME            DEFAULT NULL,
    lock_in_time    DATETIME            DEFAULT NULL,

    PRIMARY KEY (pick_id),

    FOREIGN KEY (player_id)
        REFERENCES Players(player_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    FOREIGN KEY (pick)
        REFERENCES Teams(team_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE Standings_{YEAR} (
    player_id       INT UNSIGNED        NOT NULL,
    wins            TINYINT UNSIGNED    NOT NULL DEFAULT 0,
    losses          TINYINT UNSIGNED    NOT NULL DEFAULT 0,
    win_percentage  DECIMAL(4,3)        NOT NULL DEFAULT 0.000,
    ats_points      INT                 NOT NULL DEFAULT 0,
    streak          VARCHAR(4)          NOT NULL DEFAULT '-',

    PRIMARY KEY (player_id),

    FOREIGN KEY (player_id)
        REFERENCES Players(player_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE Standings_Message_{YEAR} (
   message_id      INT UNSIGNED        NOT NULL AUTO_INCREMENT,
   week            TINYINT UNSIGNED    NOT NULL,
   message         TEXT                NOT NULL,

   PRIMARY KEY (message_id)
) ENGINE=InnoDB;

ALTER TABLE Players
ADD COLUMN `{YEAR}_registration` TINYINT UNSIGNED DEFAULT NULL;

ALTER TABLE Players
ADD COLUMN `{YEAR}_paid` TINYINT UNSIGNED DEFAULT NULL;
