# Dr Tin Chatbot - Detailed Analysis

**Analysis Date:** 2025-10-06 13:19:07

**Source URL:** https://www.hko.gov.hk/en/education/weather/data-and-technology/00569-How-Chatbot-Dr-Tin-is-Trained.html#myCarousel-2

## Dr Tin Chatbot Information

### Launch Information

- launched in february 2020
- february 2020

### Statistics

- **(\d+)\s*thousands?\s*of\s*dialogues?**: 120 thousands of dialogues
- **rating\s*of\s*(\d+)**: rating of 4

### Features

- weather\s*forecast
- weather\s*warning
- tidal\s*information
- sunrise.*sunset
- astronomy
- current\s*weather

### Technical Details

- artificial intelligence
- ai
- nlu
- natural language
- supervised learning
- tokenization

### Dr Tin Mentions

1. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** How Chatbot “Dr Tin” is Trained?｜Hong Kong Observatory(HKO)｜Educational Resources











































Skip Content


















How Chatbot “Dr Tin” is Trained?

2. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** How Chatbot “Dr Tin” is Trained?｜Hong Kong Observatory(HKO)｜Educational Resources











































Skip Content


















How Chatbot “Dr Tin” is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service

3. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** tory(HKO)｜Educational Resources











































Skip Content


















How Chatbot “Dr Tin” is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service "Dr Tin" which employs artificial intelligence (AI) to automatically answer a se

4. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service "Dr Tin" which employs artificial intelligence (AI) to automatically answer a series of weather and astronomy related questions such as current weather, weather warnings, weather forecast, tidal information,

5. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** hatbot was well received with a monthly average of about 120 thousands of dialogues, and a rating of 4 or above out of 5 since launched. So, what is the working principle behind the chatbot? How does Dr Tin understand and respond to questions?
First of all, Dr Tin will classify the questions. HKO will prepare a batch of sample questions and their respective intents. Then a computer program will generate

6. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** 20 thousands of dialogues, and a rating of 4 or above out of 5 since launched. So, what is the working principle behind the chatbot? How does Dr Tin understand and respond to questions?
First of all, Dr Tin will classify the questions. HKO will prepare a batch of sample questions and their respective intents. Then a computer program will generate a learning model by applying supervised learning on the s

7. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** nized questions and their respective intent, i.e. a well-prepared training data set. With the use of AI program, a learning model for classifying questions can be developed.
When there is a question, Dr Tin will use the learning model to find out a number of intents that the enquiry most probably belongs to, and assign the question to the intent with the highest score. For example, if someone asks "Toda

8. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** o find out a number of intents that the enquiry most probably belongs to, and assign the question to the intent with the highest score. For example, if someone asks "Today’s temperature of Sha Tin?", Dr Tin will first find out the tokens of the question, and use the prepared learning model to find out the score of the question associated with each intent. The intent possessing the highest score would be

9. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** today", "temperature"


Intent:
score


(1) Temperature:
99.99…


(2) Irrelevant question:
0.00…


(3) Sunrise and sunset:
0.00…


(4) ....




 
Since the intent "temperature" has the highest score, Dr Tin will take "temperature" as the intent of the question.
Finally, the chatbot will further extract entities from the questions to prepare an answer. In the example above, "Sha Tin", "today" and "temper

10. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** questions to prepare an answer. In the example above, "Sha Tin", "today" and "temperature" are the values of entities "location", "time", and "temperature" respectively. As "today" is a  time period, Dr Tin will find out today’s maximum and minimum temperature of Sha Tin to prepare the answer for the user.

Fig. 1: A sample dialogue with "Dr Tin" in chatbot

Fig. 2: Work flow of the chatbot

11. **Pattern:** `dr\s+tin`
   **Match:** `dr tin`
   **Context:** "temperature" respectively. As "today" is a  time period, Dr Tin will find out today’s maximum and minimum temperature of Sha Tin to prepare the answer for the user.

Fig. 1: A sample dialogue with "Dr Tin" in chatbot

Fig. 2: Work flow of the chatbot








 





Related Articles







Previous



Next

12. **Pattern:** `chatbot.*dr\s+tin`
   **Match:** `chatbot “dr tin`
   **Context:** How Chatbot “Dr Tin” is Trained?｜Hong Kong Observatory(HKO)｜Educational Resources











































Skip Content


















How Chatbot “Dr Tin” is Trained?

13. **Pattern:** `chatbot.*dr\s+tin`
   **Match:** `chatbot “dr tin`
   **Context:** How Chatbot “Dr Tin” is Trained?｜Hong Kong Observatory(HKO)｜Educational Resources











































Skip Content


















How Chatbot “Dr Tin” is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service

14. **Pattern:** `chatbot.*dr\s+tin`
   **Match:** `chatbot “dr tin`
   **Context:** g Observatory(HKO)｜Educational Resources











































Skip Content


















How Chatbot “Dr Tin” is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service "Dr Tin" which employs artificial intelligence (AI) to automatically answer a se

15. **Pattern:** `chatbot.*dr\s+tin`
   **Match:** `chatbot service "dr tin" which employs artificial intelligence (ai) to automatically answer a series of weather and astronomy related questions such as current weather, weather warnings, weather forecast, tidal information, hong kong standard time, weather forecasts of major world cities and sunrise or sunset time. the chatbot was well received with a monthly average of about 120 thousands of dialogues, and a rating of 4 or above out of 5 since launched. so, what is the working principle behind the chatbot? how does dr tin`
   **Context:** Chatbot “Dr Tin” is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service "Dr Tin" which employs artificial intelligence (AI) to automatically answer a series of weather and astronomy related questions such as current weather, weather warnings, weather forecast, tidal information, Hong Kong standard time, weather forecasts of major world cities and sunrise or sunset time. The chatbot was well received with a monthly average of about 120 thousands of dialogues, and a rating of 4 or above out of 5 since launched. So, what is the working principle behind the chatbot? How does Dr Tin understand and respond to questions?
First of all, Dr Tin will classify the questions. HKO will prepare a batch of sample questions and their respective intents. Then a computer program will generate

16. **Pattern:** `chatbot.*dr\s+tin`
   **Match:** `chatbot will further extract entities from the questions to prepare an answer. in the example above, "sha tin", "today" and "temperature" are the values of entities "location", "time", and "temperature" respectively. as "today" is a  time period, dr tin`
   **Context:** evant question:
0.00…


(3) Sunrise and sunset:
0.00…


(4) ....




 
Since the intent "temperature" has the highest score, Dr Tin will take "temperature" as the intent of the question.
Finally, the chatbot will further extract entities from the questions to prepare an answer. In the example above, "Sha Tin", "today" and "temperature" are the values of entities "location", "time", and "temperature" respectively. As "today" is a  time period, Dr Tin will find out today’s maximum and minimum temperature of Sha Tin to prepare the answer for the user.

Fig. 1: A sample dialogue with "Dr Tin" in chatbot

Fig. 2: Work flow of the chatbot

17. **Pattern:** `dr\s+tin.*chatbot`
   **Match:** `dr tin" which employs artificial intelligence (ai) to automatically answer a series of weather and astronomy related questions such as current weather, weather warnings, weather forecast, tidal information, hong kong standard time, weather forecasts of major world cities and sunrise or sunset time. the chatbot was well received with a monthly average of about 120 thousands of dialogues, and a rating of 4 or above out of 5 since launched. so, what is the working principle behind the chatbot`
   **Context:** is Trained?



































 


 







How Chatbot “Dr Tin” is Trained?

LEE Hon-ping
October 2021

The Hong Kong Observatory (HKO) launched in February 2020 the chatbot service "Dr Tin" which employs artificial intelligence (AI) to automatically answer a series of weather and astronomy related questions such as current weather, weather warnings, weather forecast, tidal information, Hong Kong standard time, weather forecasts of major world cities and sunrise or sunset time. The chatbot was well received with a monthly average of about 120 thousands of dialogues, and a rating of 4 or above out of 5 since launched. So, what is the working principle behind the chatbot? How does Dr Tin understand and respond to questions?
First of all, Dr Tin will classify the questions. HKO will prepare a batch of sample questions and their respective intents. Then a computer prog

18. **Pattern:** `dr\s+tin.*chatbot`
   **Match:** `dr tin" in chatbot`
   **Context:** "temperature" respectively. As "today" is a  time period, Dr Tin will find out today’s maximum and minimum temperature of Sha Tin to prepare the answer for the user.

Fig. 1: A sample dialogue with "Dr Tin" in chatbot

Fig. 2: Work flow of the chatbot








 





Related Articles







Previous



Next

