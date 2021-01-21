from pymongo import MongoClient
from player_name_comparator import PlayerNameComparator

shared_mongo_url = 'localhost:27017'
shared_db_name = 'athlyte_new'

mongo_url = 'mongodb://athlytetstdau:dQN*yR#jE6c1B@52.15.122.211:27017/?authSource=athlyte_test&authMechanism=SCRAM-SHA-1'
db_name = 'athlyte_test'

team_master_collection_name = 'TeamMaster'
active_rosters_collection_name = 'NewActiveRoster2'
game_schedule_collection_name = 'GameSchedules_New'

STATUS_ONE_MATCH = 0
STATUS_MULTIPLE_MATCH = 1
STATUS_NO_MATCH = 2

class ActiveRoster(object):
    def __init__(self, player_name, pos, jersey_number, player_class, player_uuid, season):
        self.player_name = player_name
        self.pos = pos
        self.jersey_number = jersey_number
        self.player_class = player_class
        self.player_uuid = player_uuid
        self.season = season

class AllRoster(object):
    def __init__(self, player_name, pos, jersey_number, player_class, player_uuid, season):
        self.player_name = player_name
        self.pos = pos
        self.jersey_number = jersey_number
        self.player_class = player_class
        self.player_uuid = player_uuid
        self.season = season

class ActiveCoach(object):
    def __init__(self, coach_name, season, pos='NA'):
        self.coach_name = coach_name
        self.pos = pos
        self.season = season

class PlayerNameMatcher(object):
    def __init__(self, team_code, season, sport_code):
        self.team_code = team_code
        self.season = season
        self.sport_code = sport_code
        self.mongo_client = MongoClient(shared_mongo_url)
        self.mongo_db = self.mongo_client[shared_db_name]
        self.mongo_collection_ar = self.mongo_db[active_rosters_collection_name]
        self.players_dict = dict()

    def _get_active_players(self):
        active_rosters_list = list()
        active_rosters = self.mongo_collection_ar.find({'$and': [{'sportCode': self.sport_code},
                                                                     {'season': self.season}, {'teamCode': self.team_code}]})
        active_rosters_list.append(active_rosters)
        active_rosters_dict = dict()

        for active_rosters in active_rosters_list:
            for active_roster in active_rosters:
                player_uuid = active_roster['playerId']
                player_pos = active_roster['position'] if 'position' in active_roster and active_roster[
                    'position'] is not None else 'NA'
                jersey_number = active_roster['jerseyNumber']
                player_class = active_roster['playerClass']
                player_name_arr = active_roster['playerName']
                season = active_roster['season']
                for player_name in player_name_arr:
                    roster = ActiveRoster(player_name, player_pos, jersey_number, player_class, player_uuid, season)
                    if player_name not in active_rosters_dict.keys():
                        active_rosters_dict[player_name] = roster
                player_name_alias_arr = active_roster['playerNameAlias'] if 'playerNameAlias' in active_roster else []
                for player_name_alias in player_name_alias_arr:
                    roster = ActiveRoster(player_name_arr[0], player_pos, jersey_number, player_class, player_uuid,
                                          season)
                    if player_name_alias not in active_rosters_dict.keys():
                        active_rosters_dict[player_name_alias] = roster
        return active_rosters_dict

    def _get_all_players(self):
        all_rosters_list = list()
        all_rosters = self.mongo_collection_ar.find({'$and': [{'sportCode': self.sport_code}, {'teamCode': self.team_code}]})
        all_rosters_list.append(all_rosters)
        all_rosters_dict = dict()

        for all_rosters in all_rosters_list:
            for all_roster in all_rosters:
                player_uuid = all_roster['playerId']
                player_pos = all_roster['position'] if 'position' in all_roster and all_roster[
                    'position'] is not None else 'NA'
                jersey_number = all_roster['jerseyNumber']
                player_class = all_roster['playerClass']
                player_name_arr = all_roster['playerName']
                season = all_roster['season']
                for player_name in player_name_arr:
                    roster = ActiveRoster(player_name, player_pos, jersey_number, player_class, player_uuid, season)
                    if player_name not in all_rosters_dict.keys():
                        all_rosters_dict[player_name] = roster
                player_name_alias_arr = all_roster['playerNameAlias'] if 'playerNameAlias' in all_roster else []
                for player_name_alias in player_name_alias_arr:
                    roster = AllRoster(player_name_arr[0], player_pos, jersey_number, player_class, player_uuid,
                                          season)
                    if player_name_alias not in all_rosters_dict.keys():
                        all_rosters_dict[player_name_alias] = roster

        return all_rosters_dict

class CoachNameMatcher(object):

    def __init__(self, team_code, season, sport_code, game_date):
        self.team_code = team_code
        self.season = season
        self.sport_code = sport_code
        self.game_date = game_date
        self.mongo_client = MongoClient(mongo_url)
        self.mongo_db = self.mongo_client[db_name]
        self.mongo_collection_gs = self.mongo_db[game_schedule_collection_name]

    def _get_coach(self):
        current_coach = self.mongo_collection_gs.find({'$and': [{'sportCode': self.sport_code},{'season': self.season}, {'teamCode': self.team_code}, {'gameDate': self.game_date}]})
        active_coach_dict = dict()
        for coaches in current_coach:
            active_coach_details = coaches['coachDetails']
            active_coach_name = active_coach_details['Name']
            # print(active_coach_name)
            played_season = coaches['season']
            active_coach= ActiveCoach(active_coach_name, played_season,'NA')
            active_coach_dict[active_coach_name] = active_coach
            # print(active_coach_dict)
        return active_coach_dict
