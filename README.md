# Mind Map

### 18. Sisteme automate bazate pe procesarea semnalelor EEG și învățare automată

Actualizat pentru 2026: extins cu BCI bazată pe imaginație motorie pentru controlul unei proteze.
Obiectiv: Obiectivul acestui proiect este dezvoltarea unei interfețe creier-calculator (BCI) care
achiziționează semnale EEG, clasifică intenția utilizatorului și transmite comanda identificată
către un dispozitiv controlat. Aplicația principală propusă este controlul unei proteze de
membru superior prin imaginația motorie a mâinii stângi și a mâinii drepte, cu un model de
clasificare care generalizează la subiecți noi fără calibrare individuală. Sunt acceptate și alte
aplicații (control robotic, conducere autonomă, automatizarea locuinței).

__Setul de date 1 – setul de date de imaginație motorie WBCIC-SHU (Yang et al., 2025)__

Înregistrări EEG multi-subiect și multi-sesiune ale imaginației motorii a mâinii stângi și a
mâinii drepte, cu 53 de canale; utilizat în ediția anterioară pentru definirea protocolului
de antrenare/validare (scenarii intra-sesiune, inter-sesiune și inter-subiect).

__Placa OpenBCI Cyton (Daisy) cu 16 canale, împreună cu casca Ultracortex Mark IV__

achiziție cu BrainFlow (interfețe Python/MATLAB) sau cu OpenBCI GUI.
• Documentație: docs.openbci.com/Cyton/CytonLanding și brainflow.readthedocs.io;
dosarele proiectelor anterioare conțin exemple de înregistrări (Fatigue state, articol
despre imaginație motorie, EEGLAB).

__Metodologie:__ 

Preprocesare în MATLAB/EEGLAB sau Python (filtrare trece-bandă și notch,
re-referențiere, segmentare în epoci, eliminarea liniei de bază, re-eșantionare); analiză
temporală, spațială și spectrală pentru confirmarea calității datelor; compararea CSP + SVM,
FBCSP + SVM, EEGNet și DeepConvNet în scenarii cu dificultate crescândă (intra-sesiune,
inter-sesiune, inter-subiect); selectarea arhitecturii cu cel mai bun compromis între
performanță și cost computațional; validare pe subiecți complet izolați de antrenare; reducerea
numărului de canale pentru a corespunde căștii disponibile; interfață cu utilizatorul
(monitorizarea predicției și a acurateței) și transmiterea comenzii către dispozitiv (de exemplu,
o proteză acționată de servomotoare prin Arduino).

__Rezultate așteptate:__

• Un clasificator de imaginație motorie independent de subiect, validat pe subiecți izolați.

• Un studiu al influenței numărului de canale asupra acurateței.

• Un lanț demonstrativ complet: EEG → preprocesare → clasificator → interfață cu
utilizatorul → proteză/robot.

![./project/data/assets/head.png](./project/data/assets/head.png)