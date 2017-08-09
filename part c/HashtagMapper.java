import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class HashtagMapper extends Mapper<Object, Text, Text, IntWritable>
{

    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

        String hashtags = prepareHashtags(value.toString());

        if (hashtags.length()>0)
        {
            languageSupport(hashtags,context);
        }

    }
    public String prepareHashtags(String str)
    {

        int hashtagStart = 0;
        int hashtagEnd = 0;
        String hashtags = "";

        hashtagStart = StringUtils.ordinalIndexOf(str,";",2) + 1;
        hashtagEnd = StringUtils.ordinalIndexOf(str,";",3);

        hashtags = " "+str.substring(hashtagStart,hashtagEnd)+" ";
        hashtags = hashtags.toLowerCase();

        return hashtags.replaceAll("[^a-z ]","");
    }
    public void languageSupport(String str, Context context) throws IOException, InterruptedException
    {
        String[] supportPhrases = {"go","team","ftw","hooray","yay","yes","woo","vamos","arriba","dale","viva","vivala","fora","boral","equipede","ol","vai","losgehts","los","vor","geh","allezles","allezla","enavant","biaoczerwoni"};

        String[][] allCountries = {{"afghanistan","af","afg","afghan"}, // A
                                   {"albania","al","alb","shqipris"},
                                   {"algeria","dz","dza","alg"},
                                   {"americansamoa","as","asm","asa"},
                                   {"americanvirginislands","isv","vi","vir","usvirginislands"},
                                   {"andorra","ad","and","andorre"},
                                   {"angola","ang","ao","ago"},
                                   {"antiguaandbarbuda","ant","ag","atg","antigua","antiguabarbuda"},
                                   {"argentina","ar","arg","argy","argentine"},
                                   {"armenia","am","arm","armenian","hayastan"},
                                   {"aruba","aru","aw","abw"},
                                   {"australia","au","aus","aussie"},
                                   {"austria","at","aut","sterreich"},
                                   {"azerbaijan","az","aze","azrbaycan"},
                                   {"bahamas","bah","bs","bhs"}, // B
                                   {"bahrain","brn","bh","bhr"},
                                   {"bangladesh","ban","bd","bgd"},
                                   {"barbados","bar","bb","brb"},
                                   {"belarus","blr","by","belarus","gudija","biearu"},
                                   {"belgium","bel","be","belgique"},
                                   {"belize","biz","bz","blz"},
                                   {"benin","bj","ben","bnin"},
                                   {"bermuda","ber","bm","bmu"},
                                   {"bhutan","bhu","bt","btn"},
                                   {"bolivia","bo","bol"},
                                   {"bosniaandherzegovina","bosnia","bih","ba"},
                                   {"botswana","bot","bw","bwa","bots"},
                                   {"brazil","bra","br","brasilia","brasil"},
                                   {"britishvirginislands","ivb","vg","vgb"},
                                   {"brunei","bru","bn","brn"},
                                   {"bulgaria","bul","bg","bgr","blgarija"},
                                   {"burkinafaso","bur","bf","bfa"},
                                   {"burundi","bdi","bi"},
                                   {"cambodia","cam","kh","khm","cambodge"}, // C
                                   {"cameroon","cm","cmr"},
                                   {"canada","can","ca"},
                                   {"capeverde","cpv","cp","capeverdean","caboverde"},
                                   {"caymanislands","cay","ky","cym","cayman"},
                                   {"centralafricanrepublic","caf","cf"},
                                   {"chad","cha","td","tcd"},
                                   {"chile","chi","cl","chl","chilli","chili"},
                                   {"china","cn","chn"},
                                   {"chinesetapei","tpe","taiwan"},
                                   {"colombia","col","co","colombian"},
                                   {"comoros","com","km"},
                                   {"congo","cgo","cg","cog"},
                                   {"cookislands","cok","ck"},
                                   {"costarica","crc","cr","cri"},
                                   {"croatia","hrvatska","cro","hr","hrv","hrvaka"},
                                   {"cuba","cub","cu"},
                                   {"cyprus","cyp","cy"},
                                   {"czechrepublic","cze","cz","czech","czechrep","esk","esko"},
                                   {"denmark","den","dk","dnk","danmark"}, // D
                                   {"djibouti","dji","dj"},
                                   {"dominicanrepublic","dom","do","republicadominica"},
                                   {"dominica","dma","dm"},
                                   {"democraticrepublicofthecongo","drcongo","congodr","cod","cd","democraticrepublicofcongo"},
                                   {"ecuador","ecu","ec"}, // E
                                   {"egypt","egy","eg","mer"},
                                   {"elsalvador","esa","sv","slv"},
                                   {"equatorialguinea","geq","gq","gnq","guineaecuatorial"},
                                   {"eritrea","eri","er"},
                                   {"estonia","est","ee","eesti","viru","estland","viro","maarjamaa","igaunija"},
                                   {"ethiopia","et","eth","habeshastan","habesha","alhabasha","habasha"},
                                   {"fiji","fij","fj","fji"}, // F
                                   {"finland","fin","fi","finn","suomi","suomentasavalta","soome"},
                                   {"france","fra","fr","franaise","lhexagone","frantzia","bleus"},
                                   {"gabon","gab","ga"}, // G
                                   {"gambia","gam","gm","gmb"},
                                   {"georgia","geo","ge","sakartvelo"},
                                   {"germany","ger","de","deu","deutschland","brd","duitsland"},
                                   {"ghana","gha","gh"},
                                   {"greatbritain","unitedkingdom","gb","uk","gbr","britain"},
                                   {"greece","gre","gr","grc","hellas","hellada","hellenicrepublic","hellenicrep"},
                                   {"grenada","grn","gd","grd"},
                                   {"guam","gum","gu"},
                                   {"guatemala","gua","gt","gtm","guate"},
                                   {"guinea","gui","gn","gin","guine"},
                                   {"guineabissau","gbs","gw","gnb","guinbissau"},
                                   {"guyana","guy","gy"},
                                   {"haiti","hai","ht","hti","hati"}, // H
                                   {"honduras","hon","hn","hnd"},
                                   {"hongkong","hkg","hk"},
                                   {"hungary","hun","hu","hungari","magyarorszg","magyar"},
                                   {"iceland","isl","is","sland","fold","frnsafold","lveldisland"}, // I
                                   {"india","ind","in","bhrat","hindustan","hind","aryavarta","bhratprajatantra","bhratavara"},
                                   {"indonesia","ina","id","idn"},
                                   {"iran","iri","ir","irn","persia","fars"},
                                   {"iraq","irq","iq","mesopotamia"},
                                   {"ireland","irl","ie","erin","banba","fodla"},
                                   {"israel","isr","il"},
                                   {"italy","ita","it","italia","italiana"},
                                   {"ivorycoast","cotedivoire","civ","ci","ctedivoire"},
                                   {"jamaica","jam","jm"}, // J
                                   {"japan","jpn","jp","nippon","yamato"},
                                   {"jordan","jor","jo"},
                                   {"kazakhstan","kaz","kz"}, // K
                                   {"kenya","ken","ke"},
                                   {"kiribati","kir","ki"},
                                   {"kuwait","kuw","kw","kwt"},
                                   {"kyrgyzstan","kgz","kg"},
                                   {"laos","lao","la"}, // L
                                   {"latvia","lat","lv","lva","latvija"},
                                   {"lebanon","lib","lb","lbn","lebnan","lubnan"},
                                   {"lesotho","les","ls","lso"},
                                   {"liberia","lbr","lr"},
                                   {"libya","lba","ly","lby"},
                                   {"liechtenstein","lie","li"},
                                   {"lithuania","ltu","lt","lietuva"},
                                   {"luxembourg","lux","lu","ltzebuerg"},
                                   {"macedonia","mkd","mk"}, // M
                                   {"madagascar","mad","mg","mdg","madagasikara"},
                                   {"malawi","maw","mw","mwi","malai"},
                                   {"malaysia","mas","my","mys","maleysia"},
                                   {"maldives","mdv","mv"},
                                   {"mali","mli","ml"},
                                   {"malta","mlt","mt"},
                                   {"marshallislands","mhl","mh","aelninmje"},
                                   {"mauritania","mtn","mr","mrt","mauritanie"},
                                   {"mauritius","mri","mu","mus","maurice"},
                                   {"mexico","mex","mx","mexican","aztln","mxico"},
                                   {"micronesia","federatedstatesofmicronesia","fsm","fs"},
                                   {"moldova","mda","md"},
                                   {"monaco","mon","mc","mco"},
                                   {"mongolia","mgl","mn","mng"},
                                   {"montenegro","mne","me","crnagora"},
                                   {"morocco","mar","ma","maghreb"},
                                   {"mozambique","moz","mz","moambique"},
                                   {"myanmar","mya","mm","mmr","burma"},
                                   {"namibia","nam","na"}, // N
                                   {"nauru","nru","nr","naoero"},
                                   {"nepal","nep","np","npl","nepla"},
                                   {"netherlands","ned","nl","nld","nederlanden","nederland","holland"},
                                   {"newzealand","nzl","nz","aotearoa"},
                                   {"nicaragua","nca","ni","nic"},
                                   {"niger","nig","ne","ner"},
                                   {"nigeria","ngr","ng","nga"},
                                   {"northkorea","prk","kp","dprk"},
                                   {"norway","nor","no","norge","noreg"},
                                   {"oman","oma","om","omn"}, // O
                                   {"pakistan","pak","pk"}, // P
                                   {"palau","plw","pw","belau"},
                                   {"panama","pan","pa","panam"},
                                   {"papuanewguinea","png","pg","papuaniugini"},
                                   {"paraguay","par","py","pry"},
                                   {"peru","per","pe"},
                                   {"philippines","phi","ph","phl","pilipinas","filipinas","philippine","filipina"},
                                   {"poland","pol","pl","polska","polsko"},
                                   {"portugal","por","pt","prt"},
                                   {"puertorico","pur","pr","pri","portorico","borikn","borinquen","borinken"},
                                   {"qatar","qat","qa"}, // Q
                                   {"romania","rou","ro","romnia"}, // R
                                   {"russianfederation","russia","ru","rus","rossiya"},
                                   {"rwanda","rwa","rw"},
                                   {"saintkittsandnevis","stkitts","stkittsandnevis","skn","kn","kna"}, // S
                                   {"saintlucia","stlucia","lca","lc"},
                                   {"saintvincentandthegrenadines","stvincent","stvincentandthegrenadines","vin","vc","vct"},
                                   {"samoa","sam","ws","wsm"},
                                   {"sanmarino","smr","sm"},
                                   {"saotomeandprincipe","stp","st","sotomeprncipe"},
                                   {"saudiarabia","ksa","sa","sau","saudi"},
                                   {"senegal","sen","sn"},
                                   {"serbia","srb","rs","srbija"},
                                   {"seychelles","sey","sc","syc"},
                                   {"sierraleone","sle","sl"},
                                   {"singapore","sin","sg","sgp","singapura"},
                                   {"slovakia","slovak","svk","sk","slovensko"},
                                   {"slovenia","slo","si","svn","slovenija"},
                                   {"solomonislands","sol","sb","slb"},
                                   {"somalia","som","so","somali","soomaaliya"},
                                   {"southafrica","rsa","za","zaf","suidafrika "},
                                   {"southkorea","kor","kr","hanguk"},
                                   {"spain","esp","es","espaa","espanya","espainia"},
                                   {"srilanka","sri","lk","lka"},
                                   {"sudan","sud","sd","sdn"},
                                   {"suriname","sur","sr"},
                                   {"swaziland","swz","sz","eswatini"},
                                   {"sweden","swe","se","sverige"},
                                   {"switzerland","sui","ch","che","swiss","schweiz","suisse","svizra","helvetia","schweizerische"},
                                   {"syria","syr","sy"},
                                   {"tajikistan","tjk","tj"}, // T
                                   {"tanzania","tan","tz","tza"},
                                   {"thailand","tha","th","thai"},
                                   {"togo","tog","tg","tgo"},
                                   {"tonga","tga","to","ton"},
                                   {"trinidadandtobago","trini","trinidad","tto","tt","trinbago","iere"},
                                   {"tunisia","tun","tn"},
                                   {"turkey","tur","tr","trkiye"},
                                   {"turkmenistan","tkm","tm"},
                                   {"tuvalu","tuv","tv"},
                                   {"uganda","ug","uga"}, // U
                                   {"ukraine","ua","ukr","ukrana"},
                                   {"unitedarabemirates","uae","ae","are"},
                                   {"unitedstates","america","usa","us"},
                                   {"uruguay","uru","uy","ury"},
                                   {"uzbekistan","uzb","uz"},
                                   {"vanuatu","van","vu","vut"}, // V
                                   {"venezuela","ven","ve"},
                                   {"vietnam","vie","vn","vnm"},
                                   {"yemen","yem","ye"}, // Y
                                   {"zambia","zam","zm","zmb"}, // Z
                                   {"zimbabwe","zim","zw","zwe"}};

        for (int i=0; i<allCountries.length; i++)
        {
            for (int j=0; j<allCountries[i].length; j++)
            {
                for (int k=0; k<supportPhrases.length; k++)
                {
                    if (str.contains(" "+supportPhrases[k]+allCountries[i][j]+" ")||str.contains(" "+allCountries[i][j]+supportPhrases[k]+" "))
                    {
                        context.write(new Text(allCountries[i][0]),one);
                    }
                }
            }
        }
    }
}
