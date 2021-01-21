from ner_custom_tagging import StanzaTagging

game_date = '10/17/2015'
team_code ='8'
sport_code = 'MFB'
season = 2015

# game_date = '10/16/2010'
# team_code ='31'
# sport_code = 'MFB'
# season = 2010

#
# game_date = '10/12/2013'
# team_code ='8'
# sport_code = 'MFB'
# season = 2013

# game_date = '09/07/2013
# team_code ='31'
# sport_code = 'MFB'
# season = 2013

# game_date = '10/28/2017'
# team_code ='697'
# sport_code = 'MFB'
# season = 2017

if __name__ == "__main__":
    tagger = StanzaTagging(team_code, season, sport_code, game_date)
    tagged_stories_output = tagger.stanza_custom_tagging()
