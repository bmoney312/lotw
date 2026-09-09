-- Create the Leagues table
CREATE TABLE Leagues (
    league_id       INT unsigned zerofill   NOT NULL AUTO_INCREMENT,
    league_name     VARCHAR(100)            NOT NULL,
    description     VARCHAR(255)            DEFAULT NULL,
    created_date    DATETIME                DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (league_id),
    UNIQUE KEY (league_name)
) ENGINE=InnoDB;

-- Create the League_Members junction table
CREATE TABLE League_Members (
    league_id       INT unsigned zerofill   NOT NULL,
    player_id       INT unsigned zerofill   NOT NULL,
    joined_date     DATETIME                DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (league_id, player_id),

    FOREIGN KEY (league_id)
        REFERENCES Leagues(league_id)
        ON DELETE CASCADE,

    FOREIGN KEY (player_id)
        REFERENCES Players(player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;
