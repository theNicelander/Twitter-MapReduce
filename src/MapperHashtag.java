import java.io.IOException;
import java.util.Arrays;         
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;        //From the other additional mapper.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper sample input
//epoch_time;tweetId;tweet(hashtag contained); device

//Example
//1469453965000;
//757570957502394369;
//This is an example text;
//<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

public class MapperHashtag extends Mapper<Object, Text, Text, IntWritable > {

    private IntWritable count   = new IntWritable(1);    //Output value  = Always 1, doesn't change
    private Text countryName        = new Text();     
    
	public String[][] CountryList = {{"Afghanistan", "AFG", "Afghanestan"}, {"Albania","Shqiperia"}, {"Algeria", "DZA", "Jazair"}, {"American Samoa", "ASM", "Samoa"}, {"Andorra", "Andorra"}, {"Angola", "AGO", "Angola"}, {"Anguilla"}, {"Antarctica"}, {"Antigua and Barbuda", "ATG", "Antigua and Barbuda"}, {"Argentina", "ARG", "Argentina"}, {"Armenia", "Hayastan"}, {"Aruba"}, {"Australia", "AUS", "Australia"}, {"Austria", "AUT", "Österreich"}, {"Azerbaijan", "AZE", "Azarbaycan"}, {"Bahamas", "BHS", "Bahamas"}, {"Bahrain", "BHR", "Bahrayn"}, {"Bangladesh"}, {"Barbados", "BRB", "Barbados"}, {"Belarus", "BLR", "Byelarus"}, {"Belgium", "BEL", "Belgie"}, {"Belize", "BLZ", "Belize"}, {"Benin", "Benin"}, {"Bermuda", "BMU", "Bermuda"}, {"Bhutan", "BTN", "Bhutan"}, {"Bolivia", "Bolivia"}, {"Bosnia and Herzegovina", "BIH", "Bosna i Hercegovina"}, {"Botswana", "BWA", "Botswana"}, {"Brazil", "BRA", "Brasil"}, {"British Indian Ocean Territory"}, {"British Virgin Islands", "VGB"}, {"Brunei", "BRN"}, {"Bulgaria", "BGR", "Bulgaria"}, {"Burkina Faso", "BFA", "Burkina Faso"}, {"Burundi", "BDI", "Burundi"}, {"Cambodia", "KHM", "Kampuchea"}, {"Cameroon", "CMR", "Cameroon"}, {"Canada", "CAN", "Canada"}, {"Cape Verde", "CPV", "Cabo Verde"}, {"Cayman Islands", "CYM", "Cayman Islands"}, {"Central African Republic", "CAF", "Republique Centrafricaine"}, {"Chad", "TCD", "Tchad"}, {"Chile", "CHL", "Chile"}, {"China", "CHN", "Zhong Guo"}, {"Christmas Island", "CXR", "Christmas Island"}, {"Cocos Islands", "CCK"}, {"Colombia", "COL", "Colombia"}, {"Comoros", "COM", "Comores"}, {"Cook Islands", "COK", "Cook Islands"}, {"Costa Rica", "CRI", "Costa Rica"}, {"Croatia", "HRV", "Hrvatska"}, {"Cuba", "CUB", "Cuba"}, {"Curacao", "CUW"}, {"Cyprus", "CYP", "Kypros"}, {"Czech Republic", "CZE", "Ceska Republika"}, {"Denmark", "DNK", "Danmark"}, {"Djibouti", "DJI", "Djibouti"}, {"Dominica", "DMA", "Dominica"}, {"Dominican Republic", "DOM", "Dominicana"}, {"East Timor", "TLS"}, {"Ecuador", "ECU", "Ecuador"}, {"Egypt", "EGY", "Misr"}, {"El Salvador", "SLV", "Salvador"}, {"Equatorial Guinea", "GNQ", "Guinea"}, {"Eritrea", "Ertra"}, {"Estonia", "Eesti"}, {"Ethiopia", "ETH", "Ityop'iya"}, {"Falkland Islands", "FLK", "Islas Malvinas"}, {"Faroe Islands", "Foroyar"}, {"Fiji", "FJI", "Fiji"}, {"Finland", "Suomen"}, {"France", "FRA", "France"}, {"Polynesia", "PYF", "Polynésie Française"}, {"Gabon", "GAB", "Gabon"}, {"Gambia", "GMB", "The Gambia"}, {"Georgia", "GEO", "Sak'art'velo"}, {"Germany", "DEU", "Deutschland"}, {"Ghana", "GHA", "Ghana"}, {"Gibraltar", "GIB", "Gibraltar"}, {"Greece", "GRC", "Ellas"}, {"Greenland", "GRL", "Kalaallit Nunaat"}, {"Grenada", "GRD", "Grenada"}, {"Guam", "GUM", "Guam"}, {"Guatemala", "GTM", "Guatemala"}, {"Guernsey", "GGY"}, {"Guinea", "Guinee"}, {"Guinea-Bissau", "GNB", "Guine-Bissau"}, {"Guyana", "GUY", "Guyana"}, {"Haiti", "HTI", "Haiti"}, {"Honduras", "HND", "Honduras"}, {"Hong Kong", "HKG", "Xianggang"}, {"Hungary", "HUN", "Magyarorszag"}, {"Iceland", "ISL", "Ísland"}, {"India", "IND", "Bharat"}, {"Indonesia", "IDN", "Indonesia"}, {"Iran", "IRN", "Iran"}, {"Iraq", "IRQ", "Iraq"}, {"Ireland", "IRL", "Éire"}, {"IsleofMan"}, {"Israel", "ISR", "Yisra'el"}, {"Italy", "ITA", "Italia"}, {"Ivory Coast", "CIV", "Cote d'Ivoire"}, {"Jamaica", "JAM", "Jamaica"}, {"Japan", "JPN", "Nippon"}, {"Jersey", "JEY"}, {"Jordan", "JOR", "Urdun"}, {"Kazakhstan", "KAZ", "Qazaqstan"}, {"Kenya", "KEN", "Kenya"}, {"Kiribati", "KIR", "Kiribas"}, {"Kosovo", "XKX", "Kosova"}, {"Kuwait", "KWT", "Al Kuwayt"}, {"Kyrgyzstan", "KGZ", "Kyrgyz"}, {"Laos", "LAO"}, {"Latvia", "LVA", "Latvija"}, {"Lebanon", "LBN", "Lubnan"}, {"Lesotho", "LSO", "Lesotho"}, {"Liberia", "LBR", "Liberia"}, {"Libya", "LBY", "Libiyah"}, {"Liechtenstein", "Liechtenstein"}, {"Lithuania", "LTU", "Lietuva"}, {"Luxembourg", "LUX", "Letzebuerg"}, {"Macau", "MAC", "Aomen"}, {"Macedonia", "MKD"}, {"Madagascar", "MDG", "Madagascar"}, {"Malawi", "MWI", "Malawi"}, {"Malaysia", "MYS", "Malaysia"}, {"Maldives", "MDV", "Dhivehi Raajje"}, {"Mali"}, {"Malta", "MLT", "Malta"}, {"Marshall Islands", "MHL", "Marshall Islands"}, {"Mauritania", "Muritaniyah"}, {"Mauritius", "Mauritius"}, {"Mayotte", "MYT", "Mayotte"}, {"Mexico", "MEX", "Mexicanos"}, {"Micronesia", "FSM"}, {"Moldova", "MDA"}, {"Monaco", "MCO", "Monaco"}, {"Mongolia", "MNG", "Mongol Uls"}, {"Montenegro", "MNE", "Crna Gora"}, {"Montserrat", "MSR", "Montserrat"}, {"Morocco", "Maghrib"}, {"Mozambique", "MOZ", "Mocambique"}, {"Myanmar", "MMR"}, {"Namibia", "NAM", "Namibia"}, {"Nauru"}, {"Nepal", "NPL", "Nepal"}, {"Netherlands", "NLD", "Holland"}, {"Netherlands Antilles", "Antillen"}, {"New Caledonia", "NCL", "Nouvelle-Calédonie"}, {"New Zealand", "NZL", "Aotearoa"}, {"Nicaragua", "NIC", "Nicaragua"}, {"Niger", "NER", "Niger"}, {"Nigeria", "NGA", "Nigeria"}, {"Niue", "NIU", "Niue"}, {"North Korea", "PRK", "Choson"}, {"Northern Mariana Islands", "MNP", "Northern Mariana Islands"}, {"Norway", "NOR", "Norge"}, {"Oman", "OMN", "Saltanat Uman"}, {"Pakistan", "PAK", "Pakistan"}, {"Palau", "PLW", "Belau"}, {"Palestine", "PSE"}, {"Panama", "PAN", "Panama"}, {"Papua New Guinea", "PNG", "Papua Niu Gini"}, {"Paraguay", "PRY", "Paraguay"}, {"Peru", "PER", "Peru"}, {"Philippines", "PHL", "Pilipinas"}, {"Pitcairn", "PCN"}, {"Poland", "POL", "Polska"}, {"Portugal", "PRT", "Portugal"}, {"Puerto Rico", "PRI", "Puerto Rico"}, {"Qatar", "QAT", "Dawlat"}, {"Congo", "COG"}, {"Reunion", "REU"}, {"Romania", "ROU", "Romania"}, {"Russia", "RUS"}, {"Rwanda", "RWA", "Rwanda"}, {"Saint Barthelemy", "BLM"}, {"Saint Helena", "SHN"}, {"Saint Kitts and Nevis", "KNA"}, {"Saint Lucia", "LCA", "Lucia"}, {"Saint Martin", "MAF"}, {"Saint Pierre and Miquelon", "SPM"}, {"Saint Vincent and the Grenadines", "VCT"}, {"Samoa", "WSM", "Samoa"}, {"San Marino", "SMR", "San Marino"}, {"Sao Tome and Principe", "STP", "Sao Tome e Principe"}, {"Saudi Arabia", "SAU", "Arabiyah"}, {"Senegal", "SEN", "Senegal"}, {"Serbia", "SRB", "Srbija"}, {"Seychelles", "SYC", "Seychelles"}, {"Sierra Leone", "SLE", "Sierra Leone"}, {"Singapore", "SGP", "Singapore"}, {"Sint Maarten", "SXM"}, {"Slovakia", "SVK"}, {"Slovenia", "SVN", "Slovenija"}, {"Solomon Islands", "SLB", "Solomon"}, {"Somalia", "Somalia"}, {"South Africa", "ZAF", "South Africa"}, {"South Korea", "KOR"}, {"South Sudan", "SSD", "South Sudan"}, {"Spain", "ESP", "España"}, {"Sri Lanka", "LKA", "Sri Lanka"}, {"Sudan", "SDN", "As-Sudan"}, {"Suriname", "SUR", "Suriname"}, {"Swaziland", "SWZ", "Swaziland"}, {"Sweden", "SWE", "Sverige"}, {"Switzerland", "CHE", "Suisse"}, {"Syria", "SYR"}, {"Taiwan", "TWN"}, {"Tajikistan", "TJK", "Tojikiston"}, {"Tanzania", "TZA"}, {"Thailand", "THA", "Prathet"}, {"Togo", "TGO", "Togolaise"}, {"Tokelau", "TKL", "Tokelau"}, {"Tonga"}, {"Trinidad and Tobago", "TTO", "Trinidad"}, {"Tunisia", "TUN", "Tunis"}, {"Turkey", "Turkiye"}, {"Turkmenistan", "TKM", "Turkmenistan"}, {"Turks and Caicos Islands", "TCA", "Turks and Caicos Islands"}, {"Tuvalu"}, {"U.S. Virgin Islands", "VIR"}, {"Uganda", "UGA", "Uganda"}, {"Ukraine", "UKR", "Ukrayina"}, {"United Arab Emirates", "ARE", "Al Imarat al Arabiyah al Muttahidah"}, {"UK", "GBR", "GB", "Britain"}, {"United States", "USA", "United States"}, {"Uruguay", "URY", "Republica Oriental del Uruguay"}, {"Uzbekistan", "UZB", "Uzbekiston"}, {"Vanuatu", "VUT", "Vanuatu"}, {"Vatican"}, {"Venezuela", "VEN", "Venezuela"}, {"Vietnam", "VNM", "Viet Nam"}, {"Wallis and Futuna", "WLF"}, {"Sahara", "ESH"}, {"Yemen", "YEM", "Yaman"}, {"Zambia", "ZMB", "Zambia"}, {"Zimbabwe", "ZWE", "Zimbabwe"}};
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //Split the data by ";" and add the splits to the array "input"
        String[] input = value.toString().split(";");
 
        //If the input doesn't have 4 objects, the data is invalid
        if (input.length == 4){
            
            //Split the body of the tweets into individual words and put each word into array "tweetBodyWords"
            String[] tweetBodyWords = input[2].toString().split(" ");
            
                
            //Initialize vector "hashtags" which will store all hashtags
            Vector<String> hashtags = new Vector<String>();
            
            try {
                for(String word : tweetBodyWords){      
                	
                	//Remove non ASCII Basic characters (except characters that are included in the country name variants)
                	//This is done so non essential characters such as ! are excluded
                	//[#BRA, #BRA!, #BRA!!!!!] will now all be #BRA
                	word = word.replaceAll("[^A-Za-z0-9#çÉñÍÖé']", "");
                	
                    //For every word in the tweet, check if the first character is '#'
                	if(word.toCharArray()[0] == '#'){
                    	                   	
                        //Check if there is more than one hashtag in the hashtag, example: "#USA#UK"
                        int hashCounter = 0;
                        for( int i=0; i<word.length(); i++ ) {
                            if( word.charAt(i) == '#' ) {
                                hashCounter++;
                            } 
                        }

                        //If word has one instance of '#', add it to the vector hashtags

                        if(hashCounter == 1){
                            hashtags.addElement(word);
                        }
                        
                        //If it has more than one '#' split it by '#' and add all splits to the vector hashtags
                        else{
                            String[] moreThanOneHashtag = word.split("(?=#)");
                            
                            //System.out.println("===DUPLICATE TWEET: " + word);
                            //System.out.println("Duplicate tweet for " + word + " was split into "+  Arrays.toString(moreThanOneHashtag));
                          
                            hashtags.addAll(Arrays.asList(moreThanOneHashtag));
                        }
                    }
                }
            
                //Find the how many countries there are 
                int countryListLength = CountryList.length;

                //Iterate through the vector of hashtags, (while there are more items in the vector)
                Iterator iter = hashtags.iterator();
                while (iter.hasNext()) {
                    //System.out.println("Tweet written to reducer: " + iter.next());      //example how to use the .next() function                	
                   
                	//Set element as the current hashtag.
                	Object element = iter.next();
                	
                	//For every country...
                    for(int countryIndex = 0; countryIndex < countryListLength; countryIndex++){
                    	
                    	//Check every variant of that country name
                    	for(int countryNameVariant = 0; countryNameVariant < CountryList[countryIndex].length; countryNameVariant++ ){
                    		
                    		
                    		String currentHashtag = element.toString();											//Convert the current hashtag to a string
                    		String currentCountry = CountryList[countryIndex][0];								//Set the current country to the first country in the array
                    		String currentCountryNameVariant = CountryList[countryIndex][countryNameVariant];	//
                    		
                    		
                    		//if the hashtag contains a variation of the countrie's name, add 1 to the count of that country
                    		if(currentHashtag.toLowerCase().contains(currentCountryNameVariant.toLowerCase())){
                    			
                    			
                    			countryName.set(currentCountry + "."+ currentHashtag.toUpperCase());
                    			context.write(countryName, count);
                    		}
                    	}
                    }  
                }
            }
            catch (Exception e)
            {
            }
        }        
    }
}