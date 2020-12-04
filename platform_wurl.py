from python_graphql_client import GraphqlClient
from datetime import datetime, timedelta
class platform_wurl:

    def __init__(self,AppIDSecret): 
        self.AppIDSecret = AppIDSecret
        self.client = GraphqlClient(endpoint="https://platform.wurl.com/graphql")

    def getEpisodesAssets(self, slug, date):

        variables = {
            "channelSlug": slug,
            "day" : date
        }
    
        query = """query EpgQuery($channelSlug: String!, $day: LineupDateTime!) {
        lineups(forBroadcast: true) {
            events(channelSlug: $channelSlug, day: $day) {
            list(page: 1, perPage: 100) {
                id
                start_at
                title
                episode {
                id
                title
                episode_number
                duration_ms
                assetPointers {
                    assetId
                  	startMs
                    durationMs
                   	title
                    isAd
                    isPromo
                    isSegment                
                }
                season {
                    season_number
                }
                series {
                    title
                }
                ratings {
                    value
                }
                nielsen {
                    genres {
                    title
                    }
                }
                externalIds {
                    id
                    serviceName
                    externalId
                }
                }
            }
            }
        }
        }"""
        # use GQL to get episodes of a specific date
        records = []
        global_start_time = ''
        global_end_time = ''
        try:
            reply = self.client.execute(query=query, variables=variables, 
                                headers={"Authorization": self.AppIDSecret})['data']['lineups']['events']['list'] 
        except Exception as e:
            if hasattr(e, 'message'):
                print('failed to query- ', variables, 'got message ', e.message)
            else:
                print('failed to query ', variables, ' - ', e)
            return records, global_start_time, global_end_time
        for item in reply:
            # add an episode
            start_time = item['start_at']
            date_time_obj = datetime.strptime(start_time[:-1], '%Y-%m-%dT%H:%M:%S.%f')
            xdate_time_obj = datetime.strptime(start_time[:-1], '%Y-%m-%dT%H:%M:%S.%f')
            if global_start_time == '':
               global_start_time = date_time_obj 
            episode = item['episode']
            end_time = date_time_obj+ timedelta(milliseconds=episode['duration_ms'])
            global_end_time = end_time
            ratings = 'ratings'
            if len(episode['ratings']) > 0:
                ratings = episode['ratings'][0]['value']
            genres = 'genres'
            if len(episode['nielsen']['genres']) > 0:
                genres = episode['nielsen']['genres'][0]['title']
            description = 'description'
            records.append( [slug,
                            'channel_id',
                            date,
                            date_time_obj.strftime("%Y-%m-%d %H:%M:%S.%f"),
                            end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),        # timestamp down to the milisec
                            'EntryType',#episode['EntryType'],
                            'video',
                            '"'+item['title']+'"',
                            '"'+episode['title']+'"',
                            episode['id'],
                            episode['episode_number'],
                            episode['season']['season_number'],
                            '"'+episode['series']['title']+'"',
                            ratings,
                            genres, 
                            'description'])#episode['description']]) 

            # add the assets of an episode
            for item in episode['assetPointers']:
                if item['isAd']:
                    asset_type = 'Ad'
                elif item['isPromo']:
                    asset_type = 'Promo'
                elif item['isSegment']:
                    asset_type = 'Segment'
                else:
                    asset_type = 'Video'
                #start_time = timedelta(milliseconds=item['startMs'])
                end_time = xdate_time_obj+ timedelta(milliseconds=item['durationMs'])
                records.append([slug,
                                'channel_id',
                                date,
                                xdate_time_obj.strftime("%Y-%m-%d %H:%M:%S.%f"),
                                end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                                'Asset',
                                asset_type,
                                '"'+episode['title']+'"',
                                '"'+item['title']+'"',
                                item['assetId'],
                                episode['episode_number'],
                                episode['season']['season_number'],
                                '"'+episode['series']['title']+'"',
                                ratings,
                                genres, 
                                'description'#asset['description'] 
                ])
                xdate_time_obj = end_time    
                
        return records, global_start_time, global_end_time

    def getChannelTimezoneBySlug(self,slug):
        variables = {
        "channelSlug": slug,
        }
        query = """query timeZoneQuery($channelSlug: String!) {channels {
        bySlug (slug: $channelSlug)
        {timeZone}
        }
        }
        """
        try:
            reply = self.client.execute(query=query, variables=variables, 
                                headers={"Authorization": self.AppIDSecret})['data']['channels']['bySlug']['timeZone'] 
        except Exception as e:
            if hasattr(e, 'message'):
                print('failed to query- ', variables, 'got message ', e.message)
            else:
                print('failed to query ', variables, ' - ', e)
            return '-'
        return reply
