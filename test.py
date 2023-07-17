from sqlalchemy import create_engine, VARCHAR, TIMESTAMP, INTEGER, BOOLEAN, FLOAT, DATE
from sqlalchemy.dialects.postgresql import UUID

{
                        "uuid": UUID,
                        "title": VARCHAR(255),
                        "link_to_page": VARCHAR(255),
                        "platform": VARCHAR(100),
                        "release_date": DATE,
                        "metacritic_score": INTEGER,
                        "user_score": FLOAT,
                        "developer": VARCHAR(255),
                        "publisher": VARCHAR(255),
                        "number_of_players": VARCHAR(50),
                        "rating": VARCHAR(10),
                        "genre": VARCHAR(255),
                        "description": VARCHAR(8000),
                        "online_flag": BOOLEAN,
                        "2D_flag": BOOLEAN,
                        "3D_flag": BOOLEAN
                    }