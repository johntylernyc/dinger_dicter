# General Project Notes and Observations
## Training Data for Traditional Log Regression
* Batter ID
* Pitch data for all pitches the batter has seen
* Pitch data for all pitches a pitcher has thrown
* Calculated features for both
* Some aggregation of those two datasets into a "game" level, since we're trying to predict the game in which a player will hit a home run not the specific pitch that they will.  
* TV: Home run / not home run 

*Notes: I will likely return to this approach after testing the heuristic.*

## Training Data for Similarity Predictions
* Batter ID 
* Pitch data for pitches the batter hit a home run 
* Feature data from the hitter summary table 
* Feature data from the pitcher summary table for the pitcher that is pitching that day 
* TV: “How well does the pitcher summary match the types of pitches the batter has hit out historically, and is the batter due?” 

**Method:** Distance or Similarity Calcuations
**Supporting Articles: **
* [Similarity in Python](https://ashukumar27.medium.com/similarity-functions-in-python-aa6dfe721035#:~:text=Similarity%20functions%20are%20used%20to%20measure%20the%20'distance'%20between%20two,small%2C%20and%20vice%2Dversa) 
* [Distance Measures for Machine Learning](https://machinelearningmastery.com/distance-measures-for-machine-learning/)
* [The Ultimate Guide to K-Means Clustering: Definition, Methods and Applications](https://www.analyticsvidhya.com/blog/2019/08/comprehensive-guide-k-means-clustering/)

**Strengths:** Heuristic, easily explainable, some amount of sme at play. 
**Weaknesses:** Heurisitc. 

# Data Acquired

## Batter Data
### homerun_batter_statcast_data: raw statcast data
* playerid 
* game_date
* player_name
* launch_speed
* launch_angle
* pitch_type
* release_speed
* p_throws

**Notes:**
* Probably need to add spin_rate to this table to get a better understanding of pitch_type 

### homerun_batter_statcast_data_with_features: calculated features we think might be valuable 
* launch_speed_bin
* launch_angle_bin
* speed_diff
* angle_diff
* game_month
* days_since_last_hr
* cumulative_hr
* release_speed_bin
* pitch_type_p_throws

#### homerun_batter_statcast_data_summary: aggregate data to create a profile of home runs hit for each batter
* player_id
* player_name
* cumulative_hr_max
* launch_speed_mean
* launch_speed_std
* launch_angle_mean
* launch_angle_std
* speed_diff_mean
* angle_diff_mean
* days_since_last_hr_mean
* days_since_last_hr_std
* release_speed_mean
* release_speed_median
* proportion_of_pitch_type_[pitch hand]_[pitch type]

### Additional Opportunities with Batter Data: 
* It would be interesting to see launch speed mean/std and angle mean/std by pitch type for each batter. 

## Pitcher Data
### pitcher_pitch_statcast_data
* player_id 
* player_name 
* game_date
* pitch_type
* release_speed
* spin_rate *Needs added to the batter data*
* p_throws

**Notes:**
* Do we care about the launch_angle and launch_speed of the pitch the pitcher threw? I wonder if there's a relationship there to measure relative effectiveness of the pitcher's pitch type (i.e., a pitch type they often give up hard contact / fly balls on that relates to the pitch type the batter often hits hard or launches into the air).