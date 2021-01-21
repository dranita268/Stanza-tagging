import stanza
from pymongo import MongoClient
import pandas as pd
from player_name_comparator import PlayerNameComparator
from team_master_provider import TeamMasterProvider
from name_matcher import PlayerNameMatcher, CoachNameMatcher
from pos_tagging import PosTagger
from conferences_list import ConferencesList
from fuzzywuzzy import fuzz

mongo_url = 'mongodb://athlytetstdau:dQN*yR#jE6c1B@52.15.122.211:27017/?authSource=athlyte_test&authMechanism=SCRAM-SHA-1'
db_name = 'athlyte_test'
game_notes_collection_name = 'GameNotes'
game_schedules_collection_name = 'GameSchedules_New'

shared_mongo_url= 'localhost:27017'
shared_mongo_db= 'athlyte_new'
team_master_collection_name = 'TeamMaster'
team_games_collection_name = 'teamGames'

total_PERSON_tags = 0
total_COACH_tags = 0
total_PLAYER_tags = 0
total_PER_RESIDUALS = 0
total_RESIDUALS = 0
total_TEAM_tags = 0
total_CONF_tags = 0
total_OPPO_tags = 0
total_HOME_tags = 0
total_AWAY_tags = 0
total_NEUTRAL_tags = 0
total_STADIUM_tags = 0

game_notes_team_name = ''
game_notes_oppo_team_name = ''
game_notes_stadium = ''
game_notes_all_stadiums = []

class StanzaTagging(object):

    def __init__(self, team_code, season, sport_code, game_date):
        self.mongo_client = MongoClient(mongo_url)
        self.mongo_db = self.mongo_client[db_name]
        self.mongo_collection_gn = self.mongo_db[game_notes_collection_name]
        self.mongo_collection_gs = self.mongo_db[game_schedules_collection_name]
        self.team_code = team_code
        self.sport_code = sport_code
        self.game_date = game_date
        self.total_PERSON_tags = total_PERSON_tags
        self.total_COACH_tags = total_COACH_tags
        self.total_PLAYER_tags = total_PLAYER_tags
        self.total_PER_RESIDUALS = total_PER_RESIDUALS
        self.total_RESIDUALS = total_RESIDUALS
        self.total_TEAM_tags = total_TEAM_tags
        self.total_CONF_tags = total_CONF_tags
        self.total_OPPO_tags = total_OPPO_tags
        self.total_HOME_tags = total_HOME_tags
        self.total_AWAY_tags = total_AWAY_tags
        self.total_NEUTRAL_tags = total_NEUTRAL_tags
        self.total_STADIUM_tags = total_STADIUM_tags
        self.game_notes_team_name = game_notes_team_name
        self.game_notes_oppo_team_name = game_notes_oppo_team_name
        self.game_notes_stadium = game_notes_stadium
        self.game_notes_all_stadiums = game_notes_all_stadiums
        self.season = season

    # get the stories from GameNotes collection
    def get_game_note_stories(self):   #Reads stories from the Game notes
        game_notes = self.mongo_collection_gn.find(
            {'$and': [{'teamCode': self.team_code}, {'sportCode': self.sport_code}, {'gameDate': self.game_date}]})
        game_notes = pd.DataFrame(game_notes)
        game_notes_stories = game_notes.loc[0, 'notesText ']
        return game_notes_stories

    # get the team_name, oppo_name, and other details from GameNotes collection
    def get_game_notes_details(self):
        game_notes = self.mongo_collection_gn.find(
            {'$and': [{'teamCode': self.team_code}, {'sportCode': self.sport_code}, {'gameDate': self.game_date}]})
        game_notes = pd.DataFrame(game_notes)
        self.game_notes_team_name = game_notes.loc[0, 'teamName']
        self.game_notes_oppo_team_name = game_notes.loc[0, 'oppoTeamName']
        self.game_notes_stadium = game_notes.loc[0, 'stadium']
        game_notes_all = self.mongo_collection_gn.find({"stadium":{'$exists':True}})
        game_notes_all = pd.DataFrame(game_notes_all)
        for index in range(game_notes_all.shape[0]):
            stadium = game_notes_all.loc[index, 'stadium']
            self.game_notes_all_stadiums.append(stadium)

    # get the team_location, team_state, and other details from TeamMaster collection
    def get_team_master_details(self):
        mongo_client = MongoClient(shared_mongo_url)
        mongo_db = mongo_client[shared_mongo_db]
        mongo_collection_tm = mongo_db[team_master_collection_name]
        team_details = mongo_collection_tm.find({"teamName": self.game_notes_team_name})
        team_details = pd.DataFrame(team_details)
        team_loc = team_details.loc[0, 'teamLocation']
        team_state = team_details.loc[0, 'teamState']
        return team_loc, team_state

    # get the home and visitor team details from TeamGames collection
    def get_team_games_details(self):
        mongo_client = MongoClient(shared_mongo_url)
        mongo_db = mongo_client[shared_mongo_db]
        mongo_collection_tg = mongo_db[team_games_collection_name]

        team_games_details = mongo_collection_tg.find({"name": self.game_notes_team_name,"actualDate":self.game_date})
        team_details = pd.DataFrame(team_games_details)
        neutral_location= team_details.loc[0, 'neutralLocation']
        home_team = team_details.loc[0, 'homeTeam']
        home_team_name = home_team['name']
        visitor_team = team_details.loc[0, 'visitorTeam']
        visitor_team_name = visitor_team['name']
        return neutral_location, home_team_name, visitor_team_name

    def remove_whitespace(self, text):
        return " ".join(text.split())

    # Performs some basic clean up on the stories
    def clean_game_note_story(self, short_story):
        short_story = short_story.replace('-', ' ')
        short_story = short_story.replace("'", '')
        short_story = short_story.replace('.,', ' ,')
        short_story = short_story.replace('(', ' ')
        short_story = short_story.replace(')', ' ')
        short_story = short_story.replace('fi ', 'fi')
        short_story = StanzaTagging.remove_whitespace(self, short_story)
        return short_story

    # count all persons tags in the stories
    def count_person_tags(self, tagged_df):
        words = tagged_df.shape[0]
        for word in range(words):
            if tagged_df.loc[word, 'Tags'] in (':S-PERSON', ':B-PERSON'):
                self.total_PERSON_tags = self.total_PERSON_tags + 1
        return tagged_df

    # Remove additional tags from the stories
    def clean_unwanted_tags(self, tagged_df):
        words = tagged_df.shape[0]
        for word in range(words):
            if tagged_df.loc[word, 'Tags'] == ':O':
                tagged_df.loc[word, 'Tags'] = ''

            if tagged_df.loc[word, 'Tags'] in (
                    ':B-CARDINAL', ':I-CARDINAL', ':E-CARDINAL', ':S-CARDINAL', ':B-EVENT', ':I-EVENT', ':E-EVENT',
                    ':S-DATE', ':B-DATE', ':I-DATE', ':E-DATE', ':S-TIME',':B-TIME',':E-TIME', ':S-ORDINAL',':B-QUANTITY',':E-QUANTITY',':I-QUANTITY',':S-QUANTITY'):
                tagged_df.loc[word, 'Tags'] = ''
        return tagged_df

    # tag single word PERSON tag to COACH/PLAYER
    def tag_single_word_person(self, tag_name, tagged_df, persontype):
        tags = []
        mapper = PlayerNameComparator()
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':S-PERSON':
                    matched_name = mapper.compare_and_find_best_match(tagged_df.loc[word, 'Words'], persontype, 'NA', 'NA')
                    if matched_name:
                        tagged_df.loc[word, 'Tags'] = tag_name
                        tags.append(tagged_df.loc[word, 'Words'])
                        if tag_name == ':COACH':
                            self.total_COACH_tags+=1
                        else:
                            self.total_PLAYER_tags+=1
            except:
                pass
        return tagged_df, tags

    # tag multi word PERSON tags to COACH/PLAYER
    def tag_multi_word_person(self, tag_name, tagged_df, persontype):
        tags = []
        mapper = PlayerNameComparator()
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':B-PERSON':
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == ':E-PERSON':
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    matched_coach_name = mapper.compare_and_find_best_match(word_seq, persontype, 'NA', 'NA')
                    if matched_coach_name:
                        for i in range(count - 1):
                            tagged_df.loc[word + i, 'Tags'] = ''
                        tagged_df.loc[word + count - 1, 'Tags'] = tag_name
                        tags.append(word_seq)
                        if tag_name == ':COACH':
                            self.total_COACH_tags += 1
                        else:
                            self.total_PLAYER_tags += 1
            except:
                pass
        return tagged_df, tags

    # custom tag the PERSON tags based on Single word or Multiple Words as COACH/PLAYER/RESIDUAL
    def change_person_tags(self, tagged_df):

        player_matcher = PlayerNameMatcher(self.team_code, self.season, self.sport_code)
        coach_matcher = CoachNameMatcher(self.team_code, self.season, self.sport_code, self.game_date)

        activerosters = player_matcher._get_active_players()
        allrosters = player_matcher._get_all_players()
        activecoach = coach_matcher._get_coach()

        tagged_df, coach_tags_s = StanzaTagging.tag_single_word_person(self, ':COACH', tagged_df, activecoach)
        tagged_df, player_ar_s = StanzaTagging.tag_single_word_person(self, ':PLAYER', tagged_df, activerosters)
        tagged_df, player_allr_s = StanzaTagging.tag_single_word_person(self, ':PLAYER', tagged_df, allrosters)
        player_tags_s = player_ar_s + player_allr_s

        tagged_df, coach_tags_m = StanzaTagging.tag_multi_word_person(self, ':COACH', tagged_df, activecoach)
        tagged_df, player_ar_m = StanzaTagging.tag_multi_word_person(self, ':PLAYER', tagged_df, activerosters)
        tagged_df, player_allr_m = StanzaTagging.tag_multi_word_person(self, ':PLAYER', tagged_df, allrosters)
        player_tags_m = player_ar_m + player_allr_m

        coach_tags = coach_tags_s + coach_tags_m
        player_tags = player_tags_s + player_tags_m

        words = tagged_df.shape[0]
        for word in range(words):
            if tagged_df.loc[word, 'Tags'] == ':S-PERSON':
                tagged_df.loc[word, 'Tags'] = ':SP-RESIDUAL'
                self.total_PER_RESIDUALS = self.total_PER_RESIDUALS + 1

            if tagged_df.loc[word, 'Tags'] == ':B-PERSON':
                i = 1
                while i < 10:
                    if (tagged_df.loc[(word + i), 'Tags']) == ':E-PERSON':
                        count = i + 1
                        break;
                    i = i + 1
                tagged_df.loc[word, 'Tags'] = ':BP-RESIDUAL'
                for i in range(count - 1):
                    tagged_df.loc[word + i + 1, 'Tags'] = ':IP-RESIDUAL'
                tagged_df.loc[word + count - 1, 'Tags'] = ':EP-RESIDUAL'
                self.total_PER_RESIDUALS = self.total_PER_RESIDUALS + 1

        return tagged_df, coach_tags, player_tags

    # Change single word ORG tags to CONFERENCE
    def tag_single_word_org_conf(self, tagged_df):
        conf_tags_s = []
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':S-ORG':  # single word conference
                    org = tagged_df.loc[word, 'Words']
                    conference_dict = ConferencesList.get_conference_dict(self)
                    if org in conference_dict:
                        tagged_df.loc[word, 'Tags'] = ":CONFERENCE"
                        conf_tags_s.append(tagged_df.loc[word, 'Words'])
                        self.total_CONF_tags = self.total_CONF_tags + 1
            except:
                pass
        return tagged_df, conf_tags_s

    # Change multi word ORG tags to CONFERENCE
    def tag_multi_word_org_conf(self, tagged_df):
        conf_tags_m = []
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':B-ORG':  # Conference multiple words
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == ':E-ORG':
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        if (tagged_df.loc[(word + i), 'Words']) in ('The', 'the'):
                            continue
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    conference_dict = ConferencesList.get_conference_dict(self)
                    if word_seq in conference_dict:
                        for i in range(count - 1):
                            tagged_df.loc[word + i, 'Tags'] = ''
                        tagged_df.loc[word + count - 1, 'Tags'] = ':CONFERENCE'
                        conf_tags_m.append(word_seq)
                        self.total_CONF_tags = self.total_CONF_tags + 1
            except:
                pass
        return tagged_df, conf_tags_m

    # Change multi word ORG tags to HOME/AWAY/NEUTRAL
    def tag_multi_word_org(self, tagged_df):
        home_org_tags = []
        away_org_tags = []
        neutral_org_tags = []
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':B-ORG':
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == ':E-ORG':
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        if (tagged_df.loc[(word + i), 'Words']) in ('The', 'the'):
                            continue
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    neutral_location, home_team_name, visitor_team_name = StanzaTagging.get_team_games_details(self)
                    if word_seq == self.game_notes_stadium:
                        if home_team_name == self.game_notes_team_name:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':HOME'
                            home_org_tags.append(home_team_name)
                            self.total_HOME_tags = self.total_HOME_tags + 1
                        if visitor_team_name == self.game_notes_team_name:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':AWAY'
                            away_org_tags.append(visitor_team_name)
                            self.total_AWAY_tags = self.total_AWAY_tags + 1
                        if neutral_location == True:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':NEUTRAL'
                            neutral_org_tags.append(word_seq)
                            self.total_NEUTRAL_tags = self.total_NEUTRAL_tags + 1
            except:
                pass
        return tagged_df, home_org_tags, away_org_tags, neutral_org_tags

    # Change single word ORG tags to TEAM/OPPONENT
    def tag_single_word_org_team_oppo(self, tagged_df, tag_type):
        team_org_tags = []
        oppo_org_tags = []
        team_master_provider = TeamMasterProvider(
            shared_mongo_url,
            shared_mongo_db,
            team_master_collection_name,
            logger='NA')
        words = tagged_df.shape[0]
        for word in range(words):
            if tagged_df.loc[word, 'Tags'] == tag_type:
                try:
                    org = tagged_df.loc[word, 'Words']
                    team_details = team_master_provider.get_team_data_from_master(
                        team_name=org,
                        team_code=None,
                        team_id=None)
                    team_name_of_org = team_details['teamName']
                    if team_name_of_org == self.game_notes_team_name:
                        tagged_df.loc[word, 'Tags'] = ":TEAM"
                        team_org_tags.append(tagged_df.loc[word, 'Words'])
                        self.total_TEAM_tags = self.total_TEAM_tags + 1
                    else:
                        tagged_df.loc[word, 'Tags'] = ":OPPONENT"
                        oppo_org_tags.append(tagged_df.loc[word, 'Words'])
                        self.total_OPPO_tags = self.total_OPPO_tags + 1

                except Exception:
                    tagged_df.loc[word, 'Tags'] = ":S-RESIDUAL"
                    self.total_RESIDUALS = self.total_RESIDUALS + 1

        return tagged_df, team_org_tags, oppo_org_tags

    # Change multi word ORG tags to TEAM/OPPONENT
    def tag_multi_word_org_team_oppo(self, tagged_df, b_tag_type, e_tag_type):
        team_org_tags = []
        oppo_org_tags = []
        team_master_provider = TeamMasterProvider(
            shared_mongo_url,
            shared_mongo_db,
            team_master_collection_name,
            logger='NA')
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == b_tag_type:
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == e_tag_type:
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        if (tagged_df.loc[(word + i), 'Words']) in ('The', 'the'):
                            continue
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    try:
                        team_details = team_master_provider.get_team_data_from_master(team_name=word_seq, team_code=None, team_id=None)
                        team_name_of_org = team_details['teamName']
                        if team_name_of_org == self.game_notes_team_name:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':TEAM'
                            team_org_tags.append(team_name_of_org)
                            self.total_TEAM_tags = self.total_TEAM_tags + 1
                        else:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':OPPONENT'
                            oppo_org_tags.append(team_name_of_org)
                            self.total_OPPO_tags = self.total_OPPO_tags + 1

                    except Exception:
                        tagged_df.loc[word, 'Tags'] = ':B-RESIDUAL'
                        for i in range(count - 1):
                            tagged_df.loc[word + i + 1, 'Tags'] = ':I-RESIDUAL'
                        tagged_df.loc[word + count - 1, 'Tags'] = ':E-RESIDUAL'
                        self.total_RESIDUALS = self.total_RESIDUALS + 1
            except:
                pass
        return tagged_df, team_org_tags, oppo_org_tags

    # Change all the ORG tags to CONFERENCE/TEAM/OPPONENT/HOME/AWAY/NEUTRAL/RESIDUALS
    def change_org_tags(self, tagged_df):

        tagged_df, conf_tags_s = StanzaTagging.tag_single_word_org_conf(self, tagged_df)
        tagged_df, conf_tags_m = StanzaTagging.tag_multi_word_org_conf(self, tagged_df)
        conf_tags = conf_tags_s + conf_tags_m

        tagged_df, home_org_tags, away_org_tags, neutral_org_tags = StanzaTagging.tag_multi_word_org(self, tagged_df)

        tagged_df, team_org_tags_o, oppo_org_tags_o = StanzaTagging.tag_single_word_org_team_oppo(self, tagged_df, ':S-ORG')
        tagged_df, team_org_tags_p, oppo_org_tags_p = StanzaTagging.tag_single_word_org_team_oppo(self, tagged_df, ':S-PRODUCT')
        tagged_df, team_org_tags_w, oppo_org_tags_w = StanzaTagging.tag_single_word_org_team_oppo(self, tagged_df, 'S-WORK_OF_ART')
        tagged_df, team_org_tags_pr, oppo_org_tags_pr = StanzaTagging.tag_single_word_org_team_oppo(self, tagged_df, ':SP-RESIDUAL')
        team_org_tags_s = team_org_tags_o + team_org_tags_p + team_org_tags_w + team_org_tags_pr
        oppo_org_tags_s = oppo_org_tags_o + oppo_org_tags_p + oppo_org_tags_w + oppo_org_tags_pr

        tagged_df, team_org_tags_o, oppo_org_tags_o = StanzaTagging.tag_multi_word_org_team_oppo(self, tagged_df, ':B-ORG', ':E-ORG')
        tagged_df, team_org_tags_p, oppo_org_tags_p = StanzaTagging.tag_multi_word_org_team_oppo(self, tagged_df, ':B-PRODUCT', ':E-PRODUCT')
        tagged_df, team_org_tags_w, oppo_org_tags_w = StanzaTagging.tag_multi_word_org_team_oppo(self, tagged_df, 'B-WORK_OF_ART', 'E-WORK_OF_ART')
        tagged_df, team_org_tags_pr, oppo_org_tags_pr = StanzaTagging.tag_multi_word_org_team_oppo(self, tagged_df, ':BP-RESIDUAL', ':EP-RESIDUAL')
        team_org_tags_m = team_org_tags_o + team_org_tags_p + team_org_tags_w + team_org_tags_pr
        oppo_org_tags_m = oppo_org_tags_o + oppo_org_tags_p + oppo_org_tags_w + oppo_org_tags_pr

        team_org_tags = team_org_tags_s + team_org_tags_m
        oppo_org_tags = oppo_org_tags_s + oppo_org_tags_m

        return tagged_df, conf_tags, team_org_tags, oppo_org_tags, home_org_tags, away_org_tags, neutral_org_tags

    # Change single word GPE tags to TEAM/OPPONENT
    def tag_single_word_gpe_team_oppo(self, tagged_df, tag_type):
        team_gpe_tags = []
        oppo_gpe_tags = []
        team_master_provider = TeamMasterProvider(
            shared_mongo_url,
            shared_mongo_db,
            team_master_collection_name,
            logger='NA')

        words = tagged_df.shape[0]
        for word in range(words):
            if tagged_df.loc[word, 'Tags'] == tag_type:
                location = tagged_df.loc[word, 'Words']
                team_loc, team_state = StanzaTagging.get_team_master_details(self)
                team_loc_dict = [team_loc, team_state]
                if location in team_loc_dict:
                    tagged_df.loc[word, 'Tags'] = ":TEAM"
                    team_gpe_tags.append(tagged_df.loc[word, 'Words'])
                    self.total_TEAM_tags = self.total_TEAM_tags + 1
                else:
                    try:
                        team_details = team_master_provider.get_team_data_from_master(
                            team_name=location,
                            team_code=None,
                            team_id=None)
                        if team_details:
                            tagged_df.loc[word, 'Tags'] = ":OPPONENT"
                            oppo_gpe_tags.append(tagged_df.loc[word, 'Words'])
                            self.total_OPPO_tags = self.total_OPPO_tags + 1
                    except Exception:
                        pass

        return tagged_df, team_gpe_tags, oppo_gpe_tags

    # Change multi word GPE tags to TEAM/OPPONENT
    def tag_multi_word_gpe_team_oppo(self, tagged_df, b_tag_type, e_tag_type):
        team_gpe_tags = []
        oppo_gpe_tags = []
        team_master_provider = TeamMasterProvider(
            shared_mongo_url,
            shared_mongo_db,
            team_master_collection_name,
            logger='NA')

        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == b_tag_type:
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == e_tag_type:
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        if (tagged_df.loc[(word + i), 'Words']) in ('The', 'the'):
                            continue
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    team_loc, team_state = StanzaTagging.get_team_master_details(self)
                    team_loc_dict = [team_loc, team_state]
                    if word_seq in team_loc_dict:
                        for i in range(count - 1):
                            tagged_df.loc[word + i, 'Tags'] = ''
                        tagged_df.loc[word + count - 1, 'Tags'] = ":TEAM"
                        team_gpe_tags.append(word_seq)
                        self.total_TEAM_tags = self.total_TEAM_tags + 1
                    else:
                        try:
                            team_details = team_master_provider.get_team_data_from_master(
                                team_name=word_seq,
                                team_code=None,
                                team_id=None)
                            if team_details:
                                for i in range(count - 1):
                                    tagged_df.loc[word + i, 'Tags'] = ''
                                tagged_df.loc[word + count - 1, 'Tags'] = ":OPPONENT"
                                oppo_gpe_tags.append(word_seq)
                                self.total_OPPO_tags = self.total_OPPO_tags + 1
                        except Exception:
                            pass
            except:
                pass

        return tagged_df, team_gpe_tags, oppo_gpe_tags

    # Change all the GPE tags to TEAM/OPPONENT/RESIDUALS
    def change_gpe_tags(self, tagged_df):

        tagged_df, team_gpe_tags_g, oppo_gpe_tags_g = StanzaTagging.tag_single_word_gpe_team_oppo(self, tagged_df, ':S-GPE')
        tagged_df, team_gpe_tags_l, oppo_gpe_tags_l = StanzaTagging.tag_single_word_gpe_team_oppo(self, tagged_df, ':S-LOC')
        tagged_df, team_gpe_tags_n, oppo_gpe_tags_n = StanzaTagging.tag_single_word_gpe_team_oppo(self, tagged_df, ':S-NORP')
        tagged_df, team_gpe_tags_r, oppo_gpe_tags_r = StanzaTagging.tag_single_word_gpe_team_oppo(self, tagged_df, ':S-RESIDUAL')
        team_gpe_tags_s = team_gpe_tags_g + team_gpe_tags_l + team_gpe_tags_n + team_gpe_tags_r
        oppo_gpe_tags_s = oppo_gpe_tags_g + oppo_gpe_tags_l + oppo_gpe_tags_n + oppo_gpe_tags_r

        tagged_df, team_gpe_tags_g, oppo_gpe_tags_g = StanzaTagging.tag_multi_word_gpe_team_oppo(self, tagged_df, ':B-GPE',':E-GPE' )
        tagged_df, team_gpe_tags_l, oppo_gpe_tags_l = StanzaTagging.tag_multi_word_gpe_team_oppo(self, tagged_df, ':B-LOC', ':E-LOC')
        tagged_df, team_gpe_tags_n, oppo_gpe_tags_n = StanzaTagging.tag_multi_word_gpe_team_oppo(self, tagged_df, ':B-NORP', ':E-NORP')
        tagged_df, team_gpe_tags_r, oppo_gpe_tags_r = StanzaTagging.tag_multi_word_gpe_team_oppo(self, tagged_df, ':B-RESIDUAL', ':E-RESIDUAL')
        team_gpe_tags_m = team_gpe_tags_g + team_gpe_tags_l + team_gpe_tags_n + team_gpe_tags_r
        oppo_gpe_tags_m = oppo_gpe_tags_g + oppo_gpe_tags_l + oppo_gpe_tags_n + oppo_gpe_tags_r

        team_gpe_tags = team_gpe_tags_s + team_gpe_tags_m
        oppo_gpe_tags = oppo_gpe_tags_s + oppo_gpe_tags_m

        words = tagged_df.shape[0]
        for word in range(words):

            try:
                if tagged_df.loc[word, 'Tags']  in (':S-GPE', ':S-LOC', 'S-NORP'):
                    tagged_df.loc[word, 'Tags'] = ':S-RESIDUAL'

                if tagged_df.loc[word, 'Tags'] in (':B-GPE', ':B-LOC', 'B-NORP'):
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) in (':E-GPE', ':E-LOC', 'E-NORP'):
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        tagged_df.loc[word, 'Tags'] = ':B-RESIDUAL'
                        for i in range(count - 1):
                            tagged_df.loc[word + i + 1, 'Tags'] = ':I-RESIDUAL'
                        tagged_df.loc[word + count - 1, 'Tags'] = ':E-RESIDUAL'
            except:
                pass

        return tagged_df, team_gpe_tags, oppo_gpe_tags

    # Change multi word fac(facility) tags to HOME/AWAY/NEUTRAL
    def tag_multi_word_fac(self, tagged_df):
        home_fac_tags = []
        away_fac_tags = []
        neutral_fac_tags = []

        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':B-FAC':
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == ':E-FAC':
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        if (tagged_df.loc[(word + i), 'Words']) in ('The', 'the'):
                            continue
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    neutral_location, home_team_name, visitor_team_name = StanzaTagging.get_team_games_details(self)
                    self.game_notes_stadium = self.game_notes_stadium.replace('-', ' ')
                    match = fuzz.WRatio(word_seq, self.game_notes_stadium)
                    if match >= 80:
                        if home_team_name == self.game_notes_team_name:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':HOME'
                            home_fac_tags.append(home_team_name)
                            self.total_HOME_tags = self.total_HOME_tags + 1
                        if visitor_team_name == self.game_notes_team_name:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':AWAY'
                            away_fac_tags.append(visitor_team_name)
                            self.total_AWAY_tags = self.total_AWAY_tags + 1
                        if neutral_location == True:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':NEUTRAL'
                            neutral_fac_tags.append(word_seq)
                            self.total_NEUTRAL_tags = self.total_NEUTRAL_tags + 1
            except:
                pass
        return tagged_df, home_fac_tags, away_fac_tags, neutral_fac_tags

    # Change multi word fac(facility) tags STADIUM
    def tag_multi_word_fac_stadium(self, tagged_df):
        stadium_tags = []
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':B-FAC':
                    word_seq = ''
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == ':E-FAC':
                            count = i + 1
                            break;
                        i = i + 1
                    for i in range(count):
                        if (tagged_df.loc[(word + i), 'Words']) in ('The', 'the'):
                            continue
                        word_seq = word_seq + tagged_df.loc[(word + i), 'Words'] + ' '
                    word_seq = word_seq.strip()
                    for stadium in self.game_notes_all_stadiums:
                        stadium = stadium.replace('-',' ')
                        stadium = stadium.replace('5TAUIUM', 'STADIUM')
                        stadium = stadium.replace('Stadium', '')
                        stadium = stadium.replace('stadium', '')
                        stadium = stadium.replace('STADIUM', '')
                        word_seq = word_seq.replace('Stadium', '')
                        word_seq = word_seq.replace('stadium', '')
                        word_seq = word_seq.replace('STADIUM', '')
                        match = fuzz.WRatio(word_seq, stadium)

                        if match >= 90:
                            for i in range(count - 1):
                                tagged_df.loc[word + i, 'Tags'] = ''
                            tagged_df.loc[word + count - 1, 'Tags'] = ':STADIUM'
                            stadium_tags.append(word_seq)
                            self.total_STADIUM_tags = self.total_STADIUM_tags + 1
            except:
                pass
        return tagged_df, stadium_tags

    # Change all the fac(facility) tags to HOME/AWAY/NEUTRAL/STADIUM/RESIDUALS
    def change_fac_tags(self, tagged_df):
        tagged_df, home_fac_tags, away_fac_tags, neutral_fac_tags = StanzaTagging.tag_multi_word_fac(self, tagged_df)
        tagged_df, stadium_tags = StanzaTagging.tag_multi_word_fac_stadium(self, tagged_df)
        words = tagged_df.shape[0]
        for word in range(words):
            try:
                if tagged_df.loc[word, 'Tags'] == ':B-FAC':
                    i = 1
                    while i < 10:
                        if (tagged_df.loc[(word + i), 'Tags']) == ':E-FAC':
                            count = i + 1
                            break;
                        i = i + 1
                    tagged_df.loc[word, 'Tags'] = ':B-RESIDUAL'
                    for i in range(count - 1):
                        tagged_df.loc[word + i + 1, 'Tags'] = ':I-RESIDUAL'
                    tagged_df.loc[word + count - 1, 'Tags'] = ':E-RESIDUAL'
                    self.total_RESIDUALS = self.total_RESIDUALS + 1
            except:
                pass
        return tagged_df, home_fac_tags, away_fac_tags, neutral_fac_tags, stadium_tags

    # combine NER and POS tagging for all the stories.
    def combine(nlp, tagged_story, nouns):
        doc = nlp(tagged_story)
        output_story = ""
        for sent in doc.sentences:
            for token in sent.words:
                output_story += token.text
                if PosTagger.is_noun(nouns, token.text):
                    output_story += ":NOUN"
                output_story += " "
        return output_story

    # convert all the Stanza tags to Athlyte tags
    def tag_story(self, nlp, short_story):
        tagged_df = pd.DataFrame(columns=['Words', 'Tags'])
        short_story = StanzaTagging.clean_game_note_story(self, short_story)
        doc = nlp(short_story)
        for sent in doc.sentences:
            for token in sent.tokens:
                tagged_df = tagged_df.append({'Words': token.text, 'Tags': ':' + str(token.ner)}, ignore_index=True)
                tagged_df = tagged_df.astype(str)

        tagged_df = StanzaTagging.count_person_tags(self, tagged_df)
        tagged_df = StanzaTagging.clean_unwanted_tags(self, tagged_df)
        tagged_df, coach_tags, player_tags = StanzaTagging.change_person_tags(self, tagged_df)
        tagged_df, conf_tags, team_org_tags, oppo_org_tags, home_org_tags, away_org_tags, neutral_org_tags = StanzaTagging.change_org_tags(self, tagged_df)
        tagged_df, team_gpe_tags, oppo_gpe_tags = StanzaTagging.change_gpe_tags(self, tagged_df)
        tagged_df, home_fac_tags, away_fac_tags, neutral_fac_tags, stadium_tags = StanzaTagging.change_fac_tags(self, tagged_df)

        team_tags = team_org_tags + team_gpe_tags
        oppo_tags = oppo_org_tags + oppo_gpe_tags
        home_tags = home_org_tags + home_fac_tags
        away_tags = away_org_tags + away_fac_tags
        neutral_tags = neutral_org_tags + neutral_fac_tags

        tagged_df["word n tags"] = tagged_df["Words"] + tagged_df["Tags"]
        tagged_story = ' '.join(tagged_df["word n tags"])
        return tagged_story, coach_tags, player_tags, conf_tags, team_tags, oppo_tags, home_tags, away_tags, neutral_tags, stadium_tags

    # Performs the custom tagging for all Athlyte tags
    def stanza_custom_tagging(self):
        nlp = stanza.Pipeline(lang='en', processors='tokenize, pos, ner')
        tagged_stories_df = pd.DataFrame(columns=['Original stories', 'Tagged stories','Coach','Players','Conference','Team','Opponent','Home','Away','Neutral','Stadium'])
        StanzaTagging.get_game_notes_details(self)
        game_stories = StanzaTagging.get_game_note_stories(self)
        stories = len(game_stories)

        for story in range(stories):
            short_story = game_stories[story]['storyContent']
            nouns = PosTagger.noun_tagger(self, nlp, short_story)
            tagged_story, coach_tags, player_tags, conf_tags, team_tags, oppo_tags, home_tags, away_tags, neutral_tags, stadium_tags  = self.tag_story(nlp, short_story)
            tagged_stories = StanzaTagging.combine(nlp, tagged_story, nouns)
            tagged_stories_df = tagged_stories_df.append({'Original stories': short_story,'Tagged stories':tagged_stories,'Coach':coach_tags,'Players':player_tags,
                                                          'Conference': conf_tags,'Team':team_tags,'Opponent':oppo_tags,'Home':home_tags,'Away':away_tags,'Neutral':neutral_tags,
                                                          'Stadium':stadium_tags}, ignore_index=True)

        print("Person", self.total_PERSON_tags)
        print("Player", self.total_PLAYER_tags)
        print("Coach", self.total_COACH_tags)
        print("PER Residuals", self.total_PER_RESIDUALS)
        print("Team", self.total_TEAM_tags)
        print("Conf", self.total_CONF_tags)
        print("Opponent", self.total_OPPO_tags)
        print("Home", self.total_HOME_tags)
        print("Away", self.total_AWAY_tags)
        print("Neutral", self.total_NEUTRAL_tags)
        print("Stadiums", self.total_STADIUM_tags)
        print("Residuals", self.total_RESIDUALS)
        file_name = 'NER_POS_TAGS_teamcode_'+ str(self.team_code)+'_season_'+ str(self.season)+str('.xlsx')
        tagged_stories_df.to_excel(file_name, sheet_name='Stories' )
        return tagged_stories
