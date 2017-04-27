import json
import scipy.spatial.distance as spdist
import pandas as pd
import numpy as np

nba_fn = '0021500492.json'

nba_fo = open(nba_fn)
nba = json.load(nba_fo)

game_events = [i for i in nba['events'] if len(i['moments']) != 0]

mom_columns = ['team_id','player_id','x','y','z']
mom_attr = ['quarter','unix_ts','game_clock_sec','shot_clock_sec','extra']

event_max = pd.DataFrame()

for e_i, ev in enumerate(game_events):
    print e_i
    frame_df = pd.DataFrame()
    ev_moments = [i for i in ev['moments']]
    ev_players = ev['home']['players'] + ev['visitor']['players']
    player_df = pd.DataFrame(ev_players).set_index('playerid')
    for m_i, ev_m in enumerate(ev_moments):
        ev_player_m = ev_m[5]
        attr_vals = ev_m[:5]
        ev_mdf = pd.DataFrame(ev_player_m,columns=mom_columns)
        ev_m_attrs = pd.DataFrame(map(lambda x: attr_vals,ev_player_m),columns=mom_attr)
        ev_m_data = pd.concat([ev_mdf,ev_m_attrs],axis=1)
        # ev_m_data['ev_moment'] = '_'.join((str(e_i),str(m_i)))
        ev_m_data['event'] = e_i
        ev_m_data['moment'] = m_i
        frame_df = pd.concat([frame_df,ev_m_data.set_index(['event','moment','player_id'])],axis=0)

    frame_df.sort_index(inplace=True)

    ball_locs = frame_df.query('player_id == -1').groupby(level=[0,1])[['x','y']].first()

    frame_df = (pd.merge(frame_df.reset_index()
                        , ball_locs
                        , left_on=['event','moment']
                        , right_index=True
                        , suffixes=['','_ball'])
                  .set_index(['event','moment','player_id']))

    frame_df['ball_dist'] = np.sqrt(
                                np.square(frame_df.x - frame_df.x_ball) +
                                np.square(frame_df.y - frame_df.y_ball)
                            )
    prev_x = frame_df.groupby(level=2).x.shift(1)
    prev_y = frame_df.groupby(level=2).y.shift(1)
    # need to consider moments where ball leaves play for distance moved
    #in_play = ~(frame_df.shot_clock_sec.isnull())
    #prev_in_play = frame_df.assign(in_play = in_play).groupby(level=2).in_play.shift(1)
    clock_run = (frame_df.game_clock_sec != frame_df.groupby(level=2).game_clock_sec.shift(-1))
    frame_df['dist_moved'] = np.sqrt(np.square(frame_df.x - prev_x) + np.square(frame_df.y - prev_y))
    frame_df.loc[~(clock_run),'dist_moved'] = np.nan
    frame_df['speed'] = frame_df['dist_moved'] * 25


    # calculate most distance moved over 1 second window (MLB sprint speed)
    frames_window = 25
    f = lambda x: x.rolling(frames_window).sum()
    spr_speed = frame_df.groupby(level=2).dist_moved.apply(f)
    max_fps = (pd.DataFrame(spr_speed.groupby(level=2).max() * (25.0 / frames_window))
                .assign(event = e_i)
                .join(player_df)
                .reset_index()
                .set_index(['event','player_id']))
    event_max = pd.concat([event_max,max_fps])

# def moment_ball_dist(mdata):
#     balldist = spdist.squareform(spdist.pdist(mdata,'euclidean'))[0]
#     return balldist
# for em in frame_df.index.levels[0]:
#     islice = pd.IndexSlice[em,:]
#     frame_df.loc[islice,'balldist'] = moment_ball_dist(frame_df.query('ev_moment == @em')[['x','y']])
