# Настройки, которые необходимо выполнить для запуска проекта через IntelliJ IDEA.


#### При стандартном запуске и сборке проект незапустится. Поэтому необходимо предварительно выполнить установку компонентов.

Можно пройтись по инструкции: https://naomi-fridman.medium.com/install-pyspark-to-run-on-jupyter-notebook-on-windows-4ec2009de21f

Сначала установить Apache Spark (https://spark.apache.org/downloads.html)
![](/img/1.png)
Выбираем release - последний, а package - для Scala

На GitHub клонируем репозиторий: https://github.com/steveloughran/winutils
Необходима папка с последним Hadoop - hadoop-3.0.0
![](/img/2.png)

Её содержимое 

![](/img/3.png)

копируем в папку bin с установленным spark-3.4.1-bin-hadoop3-scala2.13, прямо сюда в перемешку.

![](/img/4.png)

Установить jdk-1.8

![](/img/7.png)

Прописать окружение для jdk, HADOOP, SPARK

![](/img/5.png)
![](/img/6.png)


### Теперь настройка IntelliJ IDEA

Создать новый проект
Выбрать просто проект без сборщиков (New Project) выбрать Scala, sbt, выбрать jdk - тот который кстановили ранее (это jdk-1.8), выбрать версию установленной на ПК Scala.

![](/img/8.png)

После создания проекта, проверить jdk в самом проекте, если не так, то исправить, выбрать нужный jdk в sdk

![](/img/9.png)

![](/img/10.png)

В самом проекте настроить файл sbt: (Версию установленного spark можно проверить запустив его через AnacondaPrompt

![](/img/11.png)


### Теперь подождать индексации, и можно запускать код)
